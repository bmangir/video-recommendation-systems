import json
import numpy as np

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, FloatType, StructType, StructField, IntegerType

import config
from db_connector.postgres_connector import PostgresConnector
from spark import utility
from spark.utility import create_spark_session

# BERT Model Configuration
BERT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
BERT_BATCH_SIZE = 32

# Weights for behavioral signals
BEHAVIORAL_WEIGHTS = {
    "total_views": 0.10,
    "total_likes": 0.10,
    "total_dislikes": -0.05,
    "total_comments": 0.10,
    "total_shares": 0.05,
    "unique_viewers": 0.05,
    "avg_watch_percentage": 0.10,
    "completion_rate": 0.05,
    "engagement_score": 0.20,
    "trending_score": 0.30,
}

# Hybrid embedding configuration
HYBRID_CONFIG = {
    "content_dim": EMBEDDING_DIM,
    "behavioral_dim": len(BEHAVIORAL_WEIGHTS),
    "content_weight": 0.7,
    "behavioral_weight": 0.3,
}

MODEL_INFO = {
    "model_version": "emb_v2.0_bert_hybrid",
    "model_type": "sentence_bert_hybrid",
    "model_params": {
        "bert_model": BERT_MODEL_NAME,
        "content_dim": EMBEDDING_DIM,
        "behavioral_dim": HYBRID_CONFIG["behavioral_dim"],
        "content_weight": HYBRID_CONFIG["content_weight"],
        "behavioral_weight": HYBRID_CONFIG["behavioral_weight"],
        "combine": "weighted_sum+norm"
    },
    "source_features": {
        "content": True,
        "behavior": True
    }
}


def run_pipeline(spark: SparkSession):
    """Main pipeline to generate video embeddings."""

    # Trigger func
    trigger_calculate_video_stats(spark)

    # Fetch data
    videos_df, video_stats_df, categories_df = fetch_data(spark)

    # Preprocess the dfs
    video_content_df, video_behavioral_df = prepare(videos_df, video_stats_df, categories_df)

    # Vectorize contents
    content_emb_df = vectorize_contents_bert(spark, video_content_df)

    # Vectorize behavioral stats
    behavioral_emb_df = vectorize_behaviors(video_behavioral_df)

    # Run hybrid model
    combined_df = (
        content_emb_df
        .join(behavioral_emb_df, "id", "inner")
    )
    hybrid_df = build_hybrid_embeddings(combined_df)

    final_df = ready_to_insert(hybrid_df, MODEL_INFO)
    
    # Deactivate old embeddings before inserting new ones
    deactivate_old_embeddings()

    # Save to table
    utility.save_to_pg(df=final_df, table=f"{config.BATCH_OUTPUT_SCHEMA}.video_embeddings")


def deactivate_old_embeddings():
    """Set is_latest=false for all existing embeddings before inserting new ones."""

    connector = PostgresConnector()
    conn = connector.create_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {config.BATCH_OUTPUT_SCHEMA}.video_embeddings
                SET is_latest = false
                WHERE is_latest = true
            """)
        conn.commit()
        print("Deactivated old embeddings")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def trigger_calculate_video_stats(spark: SparkSession):
    """Trigger the PLSQL function to calculate video stats."""

    query = f"(SELECT calculate_video_stats(id) FROM {config.CORE_SCHEMA}.videos) AS t"

    spark.read \
        .format("jdbc") \
        .option("url", config.PG_DB_URL) \
        .option("dbtable", query) \
        .option("user", config.PG_USER) \
        .option("password", config.PG_PW) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def fetch_data(spark: SparkSession):
    """Fetch required tables from PostgresSQL."""

    videos_df = utility.read_table(
        spark,
        table=f"{config.CORE_SCHEMA}.videos",
        partition_column="id"
    )

    video_stats_df = utility.read_table(
        spark,
        table=f"{config.ACTIVITY_SCHEMA}.video_stats",
        partition_column="video_id"
    )

    categories_df = utility.read_table(
        spark,
        table=f"{config.CORE_SCHEMA}.categories"
    )

    return videos_df, video_stats_df, categories_df


def prepare(videos: DataFrame, video_stats_df: DataFrame, categories: DataFrame):
    """Prepare content and behavioral dataframes."""

    videos.cache()
    categories.cache()

    # Filter active videos
    videos = (videos
              .filter(F.col("status")=="active")
              .select("id", "title", "description", "category_id", "tags", "language"))
    categories = categories.select(F.col("id"), F.col("parent_id"), "name")

    # Build category hierarchy
    child = categories.alias("child")
    parent = categories.alias("parent")

    categories_with_parents = (
        child.join(
            parent,
            F.col("child.parent_id") == F.col("parent.id"),
            how="left"
        )
        .select(
            F.col("child.id").alias("category_id"),
            F.col("child.name").alias("child_name"),
            F.col("parent.name").alias("parent_name")
        )
    )

    videos_with_category = (
        videos
        .join(categories_with_parents, "category_id", "left")
    )

    categories.unpersist()
    videos.unpersist()

    # Weighted content emb preparing
    content_df = (
        videos_with_category
        .withColumn(
            "tags_text",
            F.when(F.col("tags").isNotNull(), F.concat_ws(" ", F.col("tags")))
            .otherwise("")
        )
        .withColumn(
            "category_text",
            F.concat_ws(" ", F.coalesce(F.col("parent_name"), F.lit("")), 
                        F.coalesce(F.col("child_name"), F.lit("")))
        )
        .withColumn(
            "combined_text",
            F.concat_ws(
                ". ",
                F.coalesce(F.col("title"), F.lit("")),
                F.coalesce(F.col("description"), F.lit("")),
                F.col("tags_text"),
                F.col("category_text")
            )
        )
        .select("id", "title", "description", "tags_text", "category_text", "combined_text")
    )

    # Prepare behavioral dataframe
    weighted_cols = [
        F.col(c) * w for c, w in BEHAVIORAL_WEIGHTS.items()
    ]

    behavioral_df = (video_stats_df
                     .withColumn("behavioral_array", F.array(*weighted_cols))
                     ).select(F.col("video_id").alias("id"), "behavioral_array")

    return content_df, behavioral_df


def vectorize_contents_bert(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Generate content embeddings using Sentence-BERT.
    """

    from sentence_transformers import SentenceTransformer
    
    # Collect to pandas
    pdf = df.select("id", "combined_text").toPandas()
    
    # Load model once
    model = SentenceTransformer(BERT_MODEL_NAME)
    
    # Prepare texts
    ids = pdf["id"].tolist()
    texts = pdf["combined_text"].fillna("").tolist()
    
    # Process in batches
    all_embeddings = []
    total_batches = (len(texts) + BERT_BATCH_SIZE - 1) // BERT_BATCH_SIZE
    
    for i in range(0, len(texts), BERT_BATCH_SIZE):
        batch_num = i // BERT_BATCH_SIZE + 1
        batch_texts = texts[i:i + BERT_BATCH_SIZE]
        
        # Encode batch
        batch_emb = model.encode(
            batch_texts,
            show_progress_bar=False,
            convert_to_numpy=True
        )
        
        # L2 normalize and collect
        for emb in batch_emb:
            norm = np.linalg.norm(emb)
            if norm > 0:
                emb = emb / norm
            all_embeddings.append(emb.tolist())
    
    # Free model memory
    del model
    
    # Create result pandas DataFrame
    result_pdf = pd.DataFrame({
        "id": ids,
        "content_embedding": all_embeddings
    })
    
    # Convert to Spark DataFrame
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("content_embedding", ArrayType(FloatType()), True)
    ])
    
    result_df = spark.createDataFrame(result_pdf, schema=schema)
    
    return result_df


def vectorize_behaviors(df: DataFrame, target_dim: int = None) -> DataFrame:
    """
    Convert behavioral metrics to embedding.
    Projects to same dimension as content embedding for combination.
    """

    if target_dim is None:
        target_dim = HYBRID_CONFIG["content_dim"]

    behavioral_dim = HYBRID_CONFIG["behavioral_dim"]

    @F.udf(ArrayType(DoubleType()))
    def project_behavioral(arr):
        if arr is None:
            return None

        arr = np.array(arr)

        # Deterministic projection matrix
        np.random.seed(42)
        projection_matrix = np.random.randn(behavioral_dim, target_dim)
        projection_matrix = projection_matrix / np.linalg.norm(projection_matrix, axis=1, keepdims=True)

        # Project to target dimension
        projected = arr @ projection_matrix

        # L2 normalize
        norm = np.linalg.norm(projected)
        if norm > 0:
            projected = projected / norm

        return projected.tolist()

    df = df.withColumn(
        "behavioral_embedding",
        project_behavioral(F.col("behavioral_array"))
    )

    return df.select("id", "behavioral_embedding")


def build_hybrid_embeddings(df: DataFrame) -> DataFrame:
    """Combine content and behavioral embeddings with weighted sum."""

    content_weight = HYBRID_CONFIG["content_weight"]
    behavioral_weight = HYBRID_CONFIG["behavioral_weight"]

    @F.udf(ArrayType(DoubleType()))
    def weighted_combine(content_emb, behavioral_emb):
        if content_emb is None or behavioral_emb is None:
            return None

        content_arr = np.array(content_emb)
        behavioral_arr = np.array(behavioral_emb)

        # Weighted sum
        combined = (content_arr * content_weight) + (behavioral_arr * behavioral_weight)

        # L2 normalize
        norm = np.linalg.norm(combined)
        if norm > 0:
            combined = combined / norm

        return combined.tolist()

    df = df.withColumn(
        "combined_embedding",
        weighted_combine(
            F.col("content_embedding"),
            F.col("behavioral_embedding")
        )
    )

    df = df.withColumn(
        "embedding_dim",
        F.lit(EMBEDDING_DIM)
    )

    return df


def ready_to_insert(df: DataFrame, model_info: dict) -> DataFrame:
    """Prepare DataFrame for database insertion."""

    return (
        df
        .withColumn("model_version", F.lit(model_info["model_version"]))
        .withColumn("model_type", F.lit(model_info["model_type"]))
        .withColumn("model_params", F.lit(json.dumps(model_info["model_params"])))
        .withColumn("source_features", F.lit(json.dumps(model_info["source_features"])))
        .withColumn("is_latest", F.lit(True))
        .select(
        F.col("id").alias("video_id"),
        "content_embedding",
        "behavioral_embedding",
        "combined_embedding",
        "embedding_dim",
        "model_version",
        "model_type",
        "model_params",
        "source_features",
        "is_latest"
    )
    )


if __name__ == "__main__":
    spark = create_spark_session("video_embeddings_bert")

    try:
        run_pipeline(spark)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()
