import json
from uuid import uuid4

import numpy as np

import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, FloatType

import config
from spark import utility
from spark.utility import create_spark_session

# BERT Model Configuration
BERT_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# User Tower Weights
USER_EMBEDDING_WEIGHTS = {
    "watch_history": 0.5,   # Most important - actual viewing behavior
    "liked_videos": 0.3,    # Strong positive signal
    "search_queries": 0.2,  # Intent signal
}

# Configuration
MAX_WATCH_HISTORY = 50      # Last N watched videos
MAX_SEARCH_QUERIES = 10     # Last N search queries
RECENCY_DECAY_LAMBDA = 0.1  # Exponential decay for recency weighting

MODEL_INFO = {
    "model_version": "user_emb_v1.0_bert",
    "model_params": {
        "bert_model": BERT_MODEL_NAME,
        "embedding_dim": EMBEDDING_DIM,
        "watch_weight": USER_EMBEDDING_WEIGHTS["watch_history"],
        "like_weight": USER_EMBEDDING_WEIGHTS["liked_videos"],
        "search_weight": USER_EMBEDDING_WEIGHTS["search_queries"],
        "max_watch_history": MAX_WATCH_HISTORY,
        "max_search_queries": MAX_SEARCH_QUERIES,
        "recency_decay": RECENCY_DECAY_LAMBDA,
    }
}


def run_pipeline(spark: SparkSession):
    """Main pipeline to generate user embeddings."""

    # Fetch data
    (watch_history_df, likes_df, searches_df, video_embeddings_df, users_df) = fetch_data(spark)

    # Compute watch history embeddings
    watch_emb_df = compute_watch_history_embedding(watch_history_df, video_embeddings_df)

    # Compute liked videos embeddings
    liked_emb_df = compute_liked_videos_embedding(likes_df, video_embeddings_df)

    # Compute search query embeddings
    search_emb_df = compute_search_embedding(spark, searches_df)

    # Get all users who have at least one signal
    all_users = (
        users_df.select("id")
        .join(
            watch_emb_df.select("user_id").union(
                liked_emb_df.select("user_id")
            ).union(
                search_emb_df.select("user_id")
            ).distinct(),
            users_df.id == F.col("user_id"),
            "inner"
        )
        .select(F.col("id").alias("user_id"))
        .distinct()
    )

    # Combine all embeddings
    combined_df = combine_user_embeddings(all_users, watch_emb_df, liked_emb_df, search_emb_df)

    # Prepare for insertion
    final_df = ready_to_insert(combined_df)

    # Save to table
    save_user_embeddings(final_df)


def fetch_data(spark: SparkSession):
    """Fetch required tables from PostgreSQL."""

    watch_history_df = utility.read_table(
        spark,
        table=f"{config.ACTIVITY_SCHEMA}.watch_history",
        partition_column="id"
    )

    likes_df = utility.read_table(
        spark,
        table=f"{config.ACTIVITY_SCHEMA}.user_likes",
        partition_column="id"
    )

    searches_df = utility.read_table(
        spark,
        table=f"{config.ACTIVITY_SCHEMA}.searches",
        partition_column="id"
    )

    video_embeddings_df = utility.read_table(
        spark,
        table=f"{config.BATCH_OUTPUT_SCHEMA}.video_embeddings",
        where_clause="is_latest = true"
    )

    users_df = utility.read_table(
        spark,
        table=f"{config.CORE_SCHEMA}.user_profiles",
        partition_column="id"
    )

    return watch_history_df, likes_df, searches_df, video_embeddings_df, users_df


def compute_watch_history_embedding(watch_history_df: DataFrame, video_embeddings_df: DataFrame) -> DataFrame:
    """
    Compute user embedding from watch history.
    Uses exponential recency weighting to give more importance to recent views.
    """

    # Get last N watched videos per user with recency rank
    window = Window.partitionBy("user_id").orderBy(F.col("last_watched_at").desc())

    recent_watches = (
        watch_history_df
        .withColumn("recency_rank", F.row_number().over(window))
        .filter(F.col("recency_rank") <= MAX_WATCH_HISTORY)
        .withColumn(
            "recency_weight",
            F.exp(-RECENCY_DECAY_LAMBDA * (F.col("recency_rank") - 1))
        )
        .select("user_id", "video_id", "recency_weight", "recency_rank")
    )

    # Join with video embeddings
    with_embeddings = (
        recent_watches
        .join(
            video_embeddings_df.select(
                F.col("video_id"),
                F.col("combined_embedding").alias("video_embedding")
            ),
            "video_id",
            "inner"
        )
    )

    # Aggregate weighted average per user
    @F.udf(ArrayType(DoubleType()))
    def weighted_average_embedding(embeddings, weights):
        if not embeddings or not weights:
            return None

        emb_array = np.array([e for e in embeddings if e is not None])
        weight_array = np.array([w for w, e in zip(weights, embeddings) if e is not None])

        if len(emb_array) == 0:
            return None

        # Normalize weights
        weight_array = weight_array / weight_array.sum()

        # Weighted average
        avg_emb = np.average(emb_array, axis=0, weights=weight_array)

        # L2 normalize
        norm = np.linalg.norm(avg_emb)
        if norm > 0:
            avg_emb = avg_emb / norm

        return avg_emb.tolist()

    result = (
        with_embeddings
        .groupBy("user_id")
        .agg(
            F.collect_list("video_embedding").alias("embeddings"),
            F.collect_list("recency_weight").alias("weights"),
            F.count("*").alias("videos_watched_count")
        )
        .withColumn(
            "watch_history_embedding",
            weighted_average_embedding(F.col("embeddings"), F.col("weights"))
        )
        .select("user_id", "watch_history_embedding", "videos_watched_count")
    )

    return result


def compute_liked_videos_embedding(likes_df: DataFrame, video_embeddings_df: DataFrame) -> DataFrame:
    """
    Compute user embedding from liked videos.
    Simple average of all liked video embeddings.
    """

    # Filter only likes (not dislikes)
    liked_videos = (
        likes_df
        .filter(F.col("like_type") == 1)
        .select("user_id", "video_id")
    )

    # Join with video embeddings
    with_embeddings = (
        liked_videos
        .join(
            video_embeddings_df.select(
                F.col("video_id"),
                F.col("combined_embedding").alias("video_embedding")
            ),
            "video_id",
            "inner"
        )
    )

    # Aggregate average per user
    @F.udf(ArrayType(DoubleType()))
    def average_embedding(embeddings):
        if not embeddings:
            return None

        emb_array = np.array([e for e in embeddings if e is not None])

        if len(emb_array) == 0:
            return None

        # Simple average
        avg_emb = np.mean(emb_array, axis=0)

        # L2 normalize
        norm = np.linalg.norm(avg_emb)
        if norm > 0:
            avg_emb = avg_emb / norm

        return avg_emb.tolist()

    result = (
        with_embeddings
        .groupBy("user_id")
        .agg(
            F.collect_list("video_embedding").alias("embeddings"),
            F.count("*").alias("videos_liked_count")
        )
        .withColumn(
            "liked_videos_embedding",
            average_embedding(F.col("embeddings"))
        )
        .select("user_id", "liked_videos_embedding", "videos_liked_count")
    )

    return result


def compute_search_embedding(spark: SparkSession, searches_df: DataFrame) -> DataFrame:
    """
    Compute user embedding from search queries.
    Concatenates recent searches and encodes with BERT.
    """

    # Get last N search queries per user
    window = Window.partitionBy("user_id").orderBy(F.col("searched_at").desc())

    recent_searches = (
        searches_df
        .filter(F.col("query").isNotNull())
        .withColumn("search_rank", F.row_number().over(window))
        .filter(F.col("search_rank") <= MAX_SEARCH_QUERIES)
    )

    # Concatenate queries per user
    concatenated_searches = (
        recent_searches
        .groupBy("user_id")
        .agg(
            F.concat_ws(" [SEP] ", F.collect_list("query")).alias("all_queries"),
            F.count("*").alias("searches_count")
        )
    )

    # BERT encode the concatenated queries
    model_name_bc = spark.sparkContext.broadcast(BERT_MODEL_NAME)

    @F.pandas_udf(ArrayType(FloatType()))
    def bert_encode_searches(queries: pd.Series) -> pd.Series:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(model_name_bc.value)

        # Encode all queries
        query_list = queries.fillna("").tolist()
        embeddings = model.encode(query_list, show_progress_bar=False)

        # L2 normalize
        results = []
        for emb in embeddings:
            norm = np.linalg.norm(emb)
            if norm > 0:
                emb = emb / norm
            results.append(emb.tolist())

        return pd.Series(results)

    result = (
        concatenated_searches
        .withColumn(
            "search_query_embedding",
            bert_encode_searches(F.col("all_queries"))
        )
        .select("user_id", "search_query_embedding", "searches_count")
    )

    return result


def combine_user_embeddings(
    all_users: DataFrame,
    watch_emb_df: DataFrame,
    liked_emb_df: DataFrame,
    search_emb_df: DataFrame) -> DataFrame:
    """
    Combine all embedding sources into final user embedding.
    Uses weighted average with fallback for missing signals.
    """

    # Join all embeddings
    combined = (
        all_users
        .join(watch_emb_df, "user_id", "left")
        .join(liked_emb_df, "user_id", "left")
        .join(search_emb_df, "user_id", "left")
        .fillna(0, subset=["videos_watched_count", "videos_liked_count", "searches_count"])
    )

    # Weighted combination with dynamic weight adjustment
    @F.udf(ArrayType(DoubleType()))
    def combine_embeddings(watch_emb, liked_emb, search_emb):
        embeddings = []
        weights = []

        base_weights = USER_EMBEDDING_WEIGHTS

        if watch_emb is not None:
            embeddings.append(np.array(watch_emb))
            weights.append(base_weights["watch_history"])

        if liked_emb is not None:
            embeddings.append(np.array(liked_emb))
            weights.append(base_weights["liked_videos"])

        if search_emb is not None:
            embeddings.append(np.array(search_emb))
            weights.append(base_weights["search_queries"])

        if not embeddings:
            return None

        # Normalize weights to sum to 1
        weights = np.array(weights)
        weights = weights / weights.sum()

        # Weighted average
        combined = np.zeros(EMBEDDING_DIM)
        for emb, w in zip(embeddings, weights):
            combined += emb * w

        # L2 normalize
        norm = np.linalg.norm(combined)
        if norm > 0:
            combined = combined / norm

        return combined.tolist()

    result = combined.withColumn(
        "user_embedding",
        combine_embeddings(
            F.col("watch_history_embedding"),
            F.col("liked_videos_embedding"),
            F.col("search_query_embedding")
        )
    )

    return result


def ready_to_insert(df: DataFrame) -> DataFrame:
    """Prepare DataFrame for database insertion."""

    return (
        df
        .withColumn("embedding_dim", F.lit(EMBEDDING_DIM))
        .withColumn("model_version", F.lit(MODEL_INFO["model_version"]))
        .withColumn("model_params", F.lit(json.dumps(MODEL_INFO["model_params"])))
        .withColumn("calculated_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "user_id",
            "user_embedding",
            "watch_history_embedding",
            "liked_videos_embedding",
            "search_query_embedding",
            "videos_watched_count",
            "videos_liked_count",
            "searches_count",
            "embedding_dim",
            "model_version",
            "model_params",
            "calculated_at",
            "updated_at"
        )
    )


def save_user_embeddings(df: DataFrame):
    """
    Save user embeddings to PostgreSQL with upsert logic.
    Uses temporary table and merge for upsert.
    """

    schema = config.BATCH_OUTPUT_SCHEMA
    staging_table = f"{schema}.user_embeddings_staging"

    # This(batch_id) is to prevent different Spark jobs running simultaneously or consecutively on the same table
    # from overwriting each other's data, upserting incorrect data, and performing safe cleanups.
    batch_id = str(uuid4())
    df_with_batch = df.withColumn("batch_id", F.lit(batch_id))

    df_with_batch.write \
        .format("jdbc") \
        .option("url", config.PG_DB_URL) \
        .option("dbtable", staging_table) \
        .option("user", config.PG_USER) \
        .option("password", config.PG_PW) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    conn = config.conn
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {schema}.user_embeddings (
                        user_id,
                        user_embedding,
                        watch_history_embedding,
                        liked_videos_embedding,
                        search_query_embedding,
                        videos_watched_count,
                        videos_liked_count,
                        searches_count,
                        model_version,
                        model_params,
                        updated_at
                    )
                    SELECT
                        user_id,
                        user_embedding,
                        watch_history_embedding,
                        liked_videos_embedding,
                        search_query_embedding,
                        videos_watched_count,
                        videos_liked_count,
                        searches_count,
                        model_version,
                        COALESCE(NULLIF(model_params, '')::jsonb, '{{}}'::jsonb),
                        CURRENT_TIMESTAMP
                    FROM {staging_table}
                    WHERE batch_id = %s
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        user_embedding = EXCLUDED.user_embedding,
                        watch_history_embedding = EXCLUDED.watch_history_embedding,
                        liked_videos_embedding = EXCLUDED.liked_videos_embedding,
                        search_query_embedding = EXCLUDED.search_query_embedding,
                        videos_watched_count = EXCLUDED.videos_watched_count,
                        videos_liked_count = EXCLUDED.videos_liked_count,
                        searches_count = EXCLUDED.searches_count,
                        model_version = EXCLUDED.model_version,
                        model_params = EXCLUDED.model_params,
                        updated_at = CURRENT_TIMESTAMP;
                """, (batch_id,))

                # Cleanup (only own batch)
                cur.execute(f"""
                    DELETE FROM {staging_table}
                    WHERE batch_id = %s
                """, (batch_id,))

        print(f"User embeddings saved successfully (batch_id={batch_id})")

    except Exception as e:
        conn.rollback()
        raise e


if __name__ == "__main__":
    # Use memory_intensive=True for BERT model
    spark = create_spark_session("user_embeddings_bert", memory_intensive=True)

    try:
        run_pipeline(spark)
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()

