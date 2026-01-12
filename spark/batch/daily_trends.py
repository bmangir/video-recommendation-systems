from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

import config
from spark import utility

# Col names to fill NA values with 0
NUMERIC_ZERO_COLS = [
    "total_views",
    "total_likes",
    "total_dislikes",
    "total_comments",
    "total_completes",
    "unique_viewers",
    "yesterday_total_views",
    "yesterday_total_likes",
]

# Weights of features to calc trend score
WEIGHTS = {
    "total_views": 0.10,
    "total_likes": 0.15,
    "completion_rate": 0.30,
    "engagement_rate": 0.15,
    "avg_watch_percentage": 0.10,
    "views_velocity": 0.10,
    "likes_velocity": 0.10,
}

def run_pipeline(spark):
    # Fetch data
    comments_df, likes_df, watch_history_df = fetch_data(spark)

    # Aggregate data to find trending score
    aggregated_df = prepare_features(comments_df, likes_df, watch_history_df)

    # Get Top N Trend videos
    trend_videos = top_N_trends(aggregated_df, N=50)

    # Save
    utility.save_to_pg(df=trend_videos, table=f"{config.BATCH_OUTPUT_SCHEMA}.daily_trends")


def fetch_data(spark: SparkSession):
    """Fetch required tables from PostgresSQL."""

    comments_df = utility.read_table(
        spark,
        table=f"{config.ACTIVITY_SCHEMA}.comments",
        partition_column="id"
    )

    likes_df = utility.read_table(
        spark,
        table=f"{config.ACTIVITY_SCHEMA}.user_likes",
        partition_column="id"
    )

    watch_history_df = utility.read_table(
        spark,
        table=f"{config.ACTIVITY_SCHEMA}.watch_history",
        partition_column="id"
    )

    return comments_df, likes_df, watch_history_df

def prepare_features(
        comments_df: DataFrame,
        likes_df: DataFrame,
        watch_history_df: DataFrame) -> DataFrame:

    now = F.current_timestamp()

    comments_df.cache()
    likes_df.cache()
    watch_history_df.cache()

    # Get at least one video id that had taken one action last 48 hours.
    base_videos_df = (
        watch_history_df
        .filter(F.col("last_watched_at") >= now - F.expr("INTERVAL 48 HOURS"))
        .select("video_id")
        .union(
            likes_df
            .filter(F.col("created_at") >= now - F.expr("INTERVAL 48 HOURS"))
            .select("video_id")
        )
        .union(
            comments_df
            .filter(F.col("updated_at") >= now - F.expr("INTERVAL 48 HOURS"))
            .select("video_id")
        )
        .distinct()
    )

    yesterday_likes_df = (likes_df.filter(
        (F.col("created_at") >= now - F.expr("INTERVAL 48 HOURS")) &
        (F.col("created_at") <  now - F.expr("INTERVAL 24 HOURS"))
    ).groupBy("video_id").agg(F.sum(F.when(F.col("like_type")==1, 1).otherwise(0)).alias("yesterday_total_likes"))
                          .select("video_id", "yesterday_total_likes"))

    yesterday_watch_df = watch_history_df.filter(
        (F.col("last_watched_at") >= now - F.expr("INTERVAL 48 HOURS")) &
        (F.col("last_watched_at") <  now - F.expr("INTERVAL 24 HOURS"))
    ).groupBy("video_id").agg(F.coalesce(F.count("*"), F.lit(0)).alias("yesterday_total_views")).select("video_id", "yesterday_total_views")

    comments_agg = (comments_df
                   .filter((F.col("updated_at") >= now - F.expr("INTERVAL 24 HOURS")) & (F.col("updated_at") <  now))
                   .groupBy("video_id")
                   .agg(F.coalesce(F.count("*"), F.lit(0)).alias("total_comments"))
                   ).select("video_id", "total_comments")

    likes_agg = (likes_df
                 .filter((F.col("created_at") >= now - F.expr("INTERVAL 24 HOURS")) & (F.col("created_at") <  now))
                 .groupBy("video_id")
                 .agg(
                    F.sum(F.when(F.col("like_type")==1, 1).otherwise(0)).alias("total_likes"),
                    F.sum(F.when(F.col("like_type")==-1, 1).otherwise(0)).alias("total_dislikes"))
                 ).select("video_id", "total_likes", "total_dislikes")

    watch_history_agg = (watch_history_df
                         .filter((F.col("last_watched_at") >= now - F.expr("INTERVAL 24 HOURS")) & (F.col("last_watched_at") < now))
                         .groupBy("video_id")
                         .agg(
                            F.count("*").alias("total_views"),
                            F.sum(F.when(F.col("is_completed")==True, 1).otherwise(0)).alias("total_completes"),
                            F.countDistinct("user_id").alias("unique_viewers"),
                            F.coalesce(F.avg(F.col("watch_percentage")), F.lit(0.0)).alias("avg_watch_percentage")
                            )
                         ).select("video_id", "total_views", "total_completes", "unique_viewers", "avg_watch_percentage")

    joined_df = (
        base_videos_df
        .join(watch_history_agg, "video_id", "left")
        .join(yesterday_watch_df, "video_id", "left")
        .join(comments_agg, "video_id", "left")
        .join(likes_agg, "video_id", "left")
        .join(yesterday_likes_df, "video_id", "left")
    )

    comments_df.unpersist()
    likes_df.unpersist()
    watch_history_df.unpersist()

    joined_df = joined_df.fillna(0, subset=NUMERIC_ZERO_COLS)

    joined_df = joined_df.fillna({
        "avg_watch_percentage": 0.0
    })

    feats_df = joined_df.withColumns({
        "engagement_rate": safe_div(
            F.col("total_likes") + F.col("total_comments"),
            F.col("total_views")
        ),
        "completion_rate": safe_div(
            F.col("total_completes"),
            F.col("total_views")
        ),
        "views_velocity": safe_div(
            F.col("total_views") - F.col("yesterday_total_views"),
            F.col("yesterday_total_views")
        ),
        "likes_velocity": safe_div(
            F.col("total_likes") - F.col("yesterday_total_likes"),
            F.col("yesterday_total_likes")
        ),
    })

    trend_score_expr = sum(
        F.col(col) * F.lit(weight)
        for col, weight in WEIGHTS.items()
    )

    feats_df = feats_df.withColumn("daily_trend_score", trend_score_expr)

    return feats_df.select(
        "video_id", "total_views", "total_likes", "total_dislikes", "total_comments", "total_completes", "unique_viewers",
        "avg_watch_percentage", "completion_rate", "engagement_rate", "views_velocity", "likes_velocity", "daily_trend_score")

def top_N_trends(df:DataFrame, N=50) -> DataFrame:
    return df.orderBy(F.col("daily_trend_score").desc()).limit(N)

def safe_div(num, denom):
    return F.when(denom != 0, num / denom).otherwise(F.lit(0.0))

if __name__ == "__main__":
    spark = utility.create_spark_session("Daily_Recommendation_Job")

    try:
        run_pipeline(spark)

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

    finally:
        spark.stop()