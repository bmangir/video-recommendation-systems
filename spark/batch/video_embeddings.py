import json
import numpy as np

from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import Word2Vec, Normalizer
from pyspark.sql.types import ArrayType, DoubleType

import config
from spark import utility
from spark.utility import create_spark_session

CONTENT_WEIGHTS = {
    "title": 0.10,
    "description": 0.05,
    "category": 0.15,
    "union_tags": 0.40,
    "language": 0.30
}

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

HYBRID_CONFIG = {
    "content_dim": 256,
    "behavioral_dim": 10,  # # of metrics in BEHAVIORAL_WEIGHTS
    "content_weight": 0.6,
    "behavioral_weight": 0.4,
}

MODEL_INFO = {
    "model_version": "emb_v1.4_hybrid",
    "model_type": "weighted_hybrid",
    "model_params": {
        "content_dim": HYBRID_CONFIG["content_dim"],
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
    # Trigger func to get latest video_stats
    trigger_calculate_video_stats(spark)

    # Fetch data
    videos_df, video_stats_df, categories_df = fetch_data(spark)

    # Prepare dfs
    video_content_df, video_behavioral_df = prepare(videos_df, video_stats_df, categories_df)

    # Vectorization
    content_emb_df = vectorize_contents(video_content_df)
    behavioral_emb_df = vectorize_behaviors(video_behavioral_df)

    combined_df = (
        content_emb_df
        .join(behavioral_emb_df, "id", "inner")
    )

    # Hybrid approach
    hybrid_df = build_hybrid_embeddings(combined_df)

    # Made ready df to insert
    final_df = ready_to_insert(hybrid_df, MODEL_INFO)

    # Save
    utility.save_to_pg(df=final_df, table=f"{config.BATCH_OUTPUT_SCHEMA}.video_embeddings")

def trigger_calculate_video_stats(spark: SparkSession):
    """
    Trigger the PLSQL function to calculate the video stats on all historical records.
    """

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
        partition_column="id"
    )

    categories_df = utility.read_table(
        spark,
        table=f"{config.CORE_SCHEMA}.categories"
    )

    return videos_df, video_stats_df, categories_df

def prepare(videos: DataFrame, video_stats_df: DataFrame, categories: DataFrame):
    videos.cache()
    categories.cache()

    videos = (videos
              .filter(F.col("status")=="active")
              .select("id", "title", "description", "category_id", "tags", "language"))
    categories = categories.select(F.col("id"), F.col("parent_id"), "name")

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
            F.col("parent.id").alias("parent_id"),
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
        .select(
            "id",
            F.split(F.lower(F.col("title")), " ").alias("title_tokens"),
            F.split(F.lower(F.col("description")), " ").alias("description_tokens"),
            F.col("tags").alias("tags_tokens"),
            F.array("parent_name", "child_name").alias("category_tokens"),
            F.array("language").alias("language_tokens")
        )
    )

    # Make array of behavioral stats
    weighted_cols = [
        F.col(c) * w for c, w in BEHAVIORAL_WEIGHTS.items()
    ]

    behavioral_df = (video_stats_df
                     .withColumn("behavioral_array", F.array(*weighted_cols))
                     ).select(F.col("video_id").alias("id"), "behavioral_array")

    return content_df, behavioral_df

def vectorize_contents(df, vector_size=256):
    # Flatten the tokens for W2V vocabulary
    df = df.withColumn(
        "all_tokens",
        F.flatten(
            F.array(
                "title_tokens",
                "description_tokens",
                "tags_tokens",
                "category_tokens",
                "language_tokens"
            )
        )
    )

    # Word2Vector Model
    w2v = Word2Vec(
        inputCol="all_tokens",
        outputCol="base_embedding",
        vectorSize=vector_size,
        minCount=1,
        seed=42
    )
    model = w2v.fit(df)

    word_vectors_df = model.getVectors()
    word_vectors_dict = {
        row["word"]: row["vector"].toArray().tolist()
        for row in word_vectors_df.collect()
    }

    # Create a broadcast var from sparkContext, to send to each clusters only once
    spark = df.sparkSession
    word_vectors_bc = spark.sparkContext.broadcast(word_vectors_dict)
    vec_size_bc = spark.sparkContext.broadcast(vector_size)

    # Broadcast the weights
    weights_bc = spark.sparkContext.broadcast({
        "title": CONTENT_WEIGHTS["title"],
        "description": CONTENT_WEIGHTS["description"],
        "tags": CONTENT_WEIGHTS["union_tags"],
        "category": CONTENT_WEIGHTS["category"],
        "language": CONTENT_WEIGHTS["language"]
    })

    @F.udf(VectorUDT())
    def compute_weighted_embedding(title_tokens, desc_tokens, tags_tokens, cat_tokens, lang_tokens):
        """
        Take all the token lists, then calculate the embeddings for each one,
        then multiply with their weights and sum up.
        """

        wv = word_vectors_bc.value
        vec_size = vec_size_bc.value
        weights = weights_bc.value

        def avg_embedding(tokens):
            """
            Calculate average embeddings for token list
            """
            if not tokens:
                return np.zeros(vec_size)

            valid_vecs = [np.array(wv[t]) for t in tokens if t in wv]
            if not valid_vecs:
                return np.zeros(vec_size)

            return np.mean(valid_vecs, axis=0)

        title_emb = avg_embedding(title_tokens) if title_tokens else np.zeros(vec_size)
        desc_emb = avg_embedding(desc_tokens) if desc_tokens else np.zeros(vec_size)
        tags_emb = avg_embedding(tags_tokens) if tags_tokens else np.zeros(vec_size)
        cat_emb = avg_embedding(cat_tokens) if cat_tokens else np.zeros(vec_size)
        lang_emb = avg_embedding(lang_tokens) if lang_tokens else np.zeros(vec_size)

        result = (
            title_emb * weights["title"] +
            desc_emb * weights["description"] +
            tags_emb * weights["tags"] +
            cat_emb * weights["category"] +
            lang_emb * weights["language"]
        )

        return Vectors.dense(result.tolist())

    # Call the UDF and calculate all embeddings
    df = df.withColumn(
        "content_embedding_raw",
        compute_weighted_embedding(
            F.col("title_tokens"),
            F.col("description_tokens"),
            F.col("tags_tokens"),
            F.col("category_tokens"),
            F.col("language_tokens")
        )
    )

    # L2 Normalize
    normalizer = Normalizer(
        inputCol="content_embedding_raw",
        outputCol="content_embedding",
        p=2.0
    )
    df = normalizer.transform(df)

    return df.select("id", "content_embedding")


def vectorize_behaviors(df, target_dim=None):
    """
    Behavioral metrikleri embedding'e dönüştürür.
    
    Args:
        df: behavioral_array sütunu içeren DataFrame
        target_dim: Hedef boyut (None ise orijinal boyut korunur)
    """
    if target_dim is None:
        target_dim = HYBRID_CONFIG["content_dim"]
    
    behavioral_dim = HYBRID_CONFIG["behavioral_dim"]
    
    # Behavioral array'i vektöre çevir ve aynı boyuta projekte et
    @F.udf(VectorUDT())
    def project_behavioral(arr):
        if arr is None:
            return None
        
        arr = np.array(arr)
        
        # Deterministic projection matrix oluştur (seed ile)
        np.random.seed(42)
        projection_matrix = np.random.randn(behavioral_dim, target_dim)
        # Ortogonal projeksiyon için normalize et
        projection_matrix = projection_matrix / np.linalg.norm(projection_matrix, axis=1, keepdims=True)
        
        # Projeksiyon: (1, behavioral_dim) @ (behavioral_dim, target_dim) = (1, target_dim)
        projected = arr @ projection_matrix
        
        return Vectors.dense(projected.tolist())
    
    df = df.withColumn(
        "behavioral_projected",
        project_behavioral(F.col("behavioral_array"))
    )
    
    # L2 Normalize
    normalizer = Normalizer(
        inputCol="behavioral_projected",
        outputCol="behavioral_embedding",
        p=2.0
    )
    df = normalizer.transform(df)
    
    return df.select("id", "behavioral_embedding")

def build_hybrid_embeddings(df):
    """
    Combine the content and behavioral embeddings with their weighted sumç
    """

    content_weight = HYBRID_CONFIG["content_weight"]
    behavioral_weight = HYBRID_CONFIG["behavioral_weight"]
    
    @F.udf(VectorUDT())
    def weighted_combine(content_vec, behavioral_vec):
        if content_vec is None or behavioral_vec is None:
            return None
        
        content_arr = np.array(content_vec.toArray())
        behavioral_arr = np.array(behavioral_vec.toArray())
        
        # Weighted sum
        combined = (content_arr * content_weight) + (behavioral_arr * behavioral_weight)
        
        return Vectors.dense(combined.tolist())

    df = df.withColumn(
        "combined_raw",
        weighted_combine(
            F.col("content_embedding"),
            F.col("behavioral_embedding")
        )
    )
    
    normalizer = Normalizer(
        inputCol="combined_raw",
        outputCol="combined_embedding",
        p=2.0
    )
    df = normalizer.transform(df)
    
    # vector to array (to store in db)
    df = _vector_to_array(df, "content_embedding", "content_embedding")
    df = _vector_to_array(df, "behavioral_embedding", "behavioral_embedding")
    df = _vector_to_array(df, "combined_embedding", "combined_embedding")
    
    # dim
    df = df.withColumn(
        "embedding_dim",
        F.size("combined_embedding")
    )
    
    return df

def ready_to_insert(df, model_info):
    # metadata of model
    df = _add_metadata(
        df,
        model_info["model_version"],
        model_info["model_type"],
        model_info["model_params"],
        model_info["source_features"]
    )

    return df.select(
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

def _vector_to_array(df, input_col, output_col):
    vector_to_array_udf = F.udf(
        lambda v: v.toArray().tolist() if v is not None else None,
        ArrayType(DoubleType())
    )

    return df.withColumn(output_col, vector_to_array_udf(F.col(input_col)))

def _add_metadata(
        df,
        model_version,
        model_type,
        model_params,
        source_features
):
    return (
        df
        .withColumn("model_version", F.lit(model_version))
        .withColumn("model_type", F.lit(model_type))
        .withColumn("model_params", F.lit(json.dumps(model_params)))
        .withColumn("source_features", F.lit(json.dumps(source_features)))
        .withColumn("is_latest", F.lit(True))
    )


spark = create_spark_session("videoemb")
run_pipeline(spark)
spark.stop()