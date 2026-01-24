from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import config
from config import MONGO_DB_NAME
from db_connector.mongo_connector import MongoConnector
from spark import utility
from spark.utility import create_spark_session

MONGO_COLLECTION = "video_catalog"

mongo_conn = MongoConnector(config.MONGO_URI)
client = mongo_conn.create_connection()


def run_pipeline(spark: SparkSession):
    # Trigger video stats calculation (ensures fresh data)
    trigger_calculate_video_stats(spark)
    
    # Fetch all required tables
    videos_df, video_stats_df, categories_df, publishers_df = fetch_data(spark)
    
    # Denormalize and transform
    catalog_df = build_denormalized_catalog(
        videos_df, 
        video_stats_df, 
        categories_df, 
        publishers_df
    )
    
    # Prepare for MongoDB
    mongo_df = prepare_for_mongodb(catalog_df)
    
    # Write to MongoDB
    write_to_mongodb(mongo_df)


def trigger_calculate_video_stats(spark: SparkSession):
    """
    Trigger the PLSQL function to calculate video stats for all videos.
    """

    query = f"(SELECT calculate_video_stats(id) FROM {config.CORE_SCHEMA}.videos WHERE status = 'active') AS t"
    
    spark.read \
        .format("jdbc") \
        .option("url", config.PG_DB_URL) \
        .option("dbtable", query) \
        .option("user", config.PG_USER) \
        .option("password", config.PG_PW) \
        .option("driver", "org.postgresql.Driver") \
        .load()


def fetch_data(spark: SparkSession):
    videos_df = utility.read_table(
        spark,
        table=f"{config.CORE_SCHEMA}.videos",
        where_clause="status = 'active'",
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
    
    # Only fetch necessary publisher fields (exclude sensitive data like password, email)
    publishers_df = utility.read_table(
        spark,
        table=f"{config.CORE_SCHEMA}.user_profiles"
    ).select(
        F.col("id").alias("publisher_id"),
        "username",
        "avatar_url",
        "subscriber_count",
        F.col("is_creator").alias("is_verified")  # Using is_creator as verified flag
    )
    
    return videos_df, video_stats_df, categories_df, publishers_df


def build_denormalized_catalog(
    videos_df: DataFrame,
    video_stats_df: DataFrame,
    categories_df: DataFrame,
    publishers_df: DataFrame) -> DataFrame:
    
    categories_df.cache()
    
    # Build category hierarchy (child -> parent)
    child_cat = categories_df.alias("child")
    parent_cat = categories_df.alias("parent")
    
    categories_with_parent = (
        child_cat.join(
            parent_cat,
            F.col("child.parent_id") == F.col("parent.id"),
            how="left"
        )
        .select(
            F.col("child.id").alias("category_id"),
            F.col("child.name").alias("category_name"),
            F.col("child.slug").alias("category_slug"),
            F.col("parent.name").alias("parent_category_name")
        )
    )
    
    # Join videos with categories
    videos_with_category = (
        videos_df
        .join(categories_with_parent, videos_df.category_id == categories_with_parent.category_id, "left")
        .drop(categories_with_parent.category_id)
    )
    
    # Join with publishers
    videos_with_publisher = (
        videos_with_category
        .join(publishers_df, videos_with_category.publisher_id == publishers_df.publisher_id, "left")
        .drop(publishers_df.publisher_id)
    )
    
    # Join with video stats
    catalog_df = (
        videos_with_publisher
        .join(video_stats_df, videos_with_publisher.id == video_stats_df.video_id, "left")
        .drop(video_stats_df.video_id)
    )
    
    categories_df.unpersist()
    
    return catalog_df


def prepare_for_mongodb(df: DataFrame) -> DataFrame:
    """
    Transform DataFrame to match MongoDB document structure.
    """

    # Helper UDF to format duration
    @F.udf(StringType())
    def format_duration(seconds):
        if seconds is None:
            return "00:00"
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{minutes}:{secs:02d}"
    
    # Calculate like_ratio
    df = df.withColumn(
        "like_ratio",
        F.when(
            (F.col("total_likes") + F.col("total_dislikes")) > 0,
            F.col("total_likes") / (F.col("total_likes") + F.col("total_dislikes"))
        ).otherwise(0.0)
    )
    
    # Build the denormalized document structure
    mongo_df = df.select(
        # MongoDB _id (using video_id as string)
        F.concat(F.lit("video_"), F.col("id").cast("string")).alias("_id"),
        F.col("id").alias("video_id"),
        "title",
        "description",
        "thumbnail_url",
        "video_url",
        F.col("duration_seconds"),
        format_duration(F.col("duration_seconds")).alias("duration_formatted"),
        F.struct(
            F.col("category_id").alias("id"),
            F.col("category_name").alias("name"),
            F.col("category_slug").alias("slug"),
            F.col("parent_category_name").alias("parent_name")
        ).alias("category"),
        F.struct(
            F.col("publisher_id").alias("id"),
            F.col("username"),
            F.col("avatar_url"),
            F.col("subscriber_count"),
            F.col("is_verified")
        ).alias("publisher"),
        "tags",
        "language",
        "age_rating",
        F.struct(
            F.coalesce(F.col("total_views"), F.lit(0)).alias("views"),
            F.coalesce(F.col("total_likes"), F.lit(0)).alias("likes"),
            F.coalesce(F.col("total_dislikes"), F.lit(0)).alias("dislikes"),
            F.coalesce(F.col("total_comments"), F.lit(0)).alias("comments"),
            F.coalesce(F.col("total_shares"), F.lit(0)).alias("shares"),
            F.coalesce(F.col("avg_watch_percentage"), F.lit(0.0)).alias("avg_watch_percentage"),
            F.coalesce(F.col("completion_rate"), F.lit(0.0)).alias("completion_rate")
        ).alias("stats"),
        F.struct(
            F.round(F.col("like_ratio"), 3).alias("like_ratio"),
            F.coalesce(F.col("engagement_score"), F.lit(0.0)).alias("engagement_score"),
            F.coalesce(F.col("trending_score"), F.lit(0.0)).alias("trending_score")
        ).alias("engagement"),
        F.struct(
            F.col("uploaded_at"),
            F.coalesce(F.col("last_calculated_at"), F.col("updated_at")).alias("last_updated")
        ).alias("timestamps")
    )
    
    return mongo_df


def write_to_mongodb(df: DataFrame):
    """
    Write DataFrame to MongoDB with upsert.
    """

    (
        df.write
        .format("mongodb")
        .option("database", MONGO_DB_NAME)
        .option("collection", MONGO_COLLECTION)
        .option("replaceDocument", "true")  # Upsert based on _id
        .option("writeConcern.w", "majority")
        .mode("append")
        .save()
    )


def ensure_indexes():
    """
    Create indexes on MongoDB collection.
    
    Note: This should ideally be run once during initial setup.
    """
    
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index([("video_id", 1)], unique=True)
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index({ "category.slug": 1 })
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index({ "publisher.id": 1 })
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index({ "stats.views": -1 })
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index({ "engagement.trending_score": -1 })
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index({ "tags": 1 })
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index({ "timestamps.uploaded_at": -1 })
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index({ "language": 1, "category.slug": 1 })
    
    # Compound index for filtering + sorting:
    client[MONGO_DB_NAME][MONGO_COLLECTION].create_index({
        "category.slug": 1, 
        "engagement.trending_score": -1 
    })


if __name__ == "__main__":
    spark = create_spark_session("mongodb_video_catalog_sync")

    try:
        run_pipeline(spark)
        ensure_indexes() # Comment after run it once
    finally:
        spark.stop()

