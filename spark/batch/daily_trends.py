from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

import config
from spark import utility

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
                   .filter((F.col("updated_at") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")) & (F.col("updated_at") <  now))
                   .groupBy("video_id")
                   .agg(F.coalesce(F.count("*"), F.lit(0)).alias("total_comments"))
                   ).select("video_id", "total_comments")

    likes_agg = (likes_df
                 .filter((F.col("created_at") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")) & (F.col("created_at") <  now))
                 .groupBy("video_id")
                 .agg(
                    F.sum(F.when(F.col("like_type")==1, 1).otherwise(0)).alias("total_likes"),
                    F.sum(F.when(F.col("like_type")==-1, 1).otherwise(0)).alias("total_dislikes"))
                 ).select("video_id", "total_likes", "total_dislikes")

    watch_history_agg = (watch_history_df
                         .filter((F.col("last_watched_at") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")) & (F.col("last_watched_at") < now))
                         .groupBy("video_id")
                         .agg(
                            F.count("*").alias("total_views"),
                            F.sum(F.when(F.col("is_completed")==True, 1).otherwise(0)).alias("total_completes"),
                            F.countDistinct("user_id").alias("unique_viewers"),
                            F.coalesce(F.avg(F.col("watch_percentage")), F.lit(0.0)).alias("avg_watch_percentage")
                            )
                         ).select("video_id", "total_views", "total_completes", "unique_viewers", "avg_watch_percentage")

    feats_df = ((watch_history_agg
                .join(comments_agg, "video_id", "left")
                .join(likes_agg, "video_id", "left")
                .join(yesterday_likes_df, "video_id", "left")
                .join(yesterday_watch_df, "video_id", "left")
                 )
                .fillna({
                        "total_views": 0,
                        "total_likes": 0,
                        "yesterday_total_views": 0,
                        "yesterday_total_likes": 0,
                        "total_dislikes": 0,
                        "total_comments": 0,
                        "total_completes": 0,
                        "unique_viewers": 0,
                        "avg_watch_percentage": 0.0
                })
                .withColumn("engagement_rate", F.when(F.col("total_views") != 0, (F.col("total_likes") + F.col("total_comments")) / F.col("total_views")).otherwise(0.0))
                .withColumn("completion_rate", F.when(F.col("total_views") != 0, F.col("total_completes") / F.col("total_views")).otherwise(0.0))
                .withColumn("views_velocity", F.when(F.col("yesterday_total_views") != 0, (F.col("total_views") - F.col("yesterday_total_views")) / F.col("yesterday_total_views")).otherwise(0.0))
                .withColumn("likes_velocity", F.when(F.col("yesterday_total_likes") != 0, (F.col("total_likes") - F.col("yesterday_total_likes")) / F.col("yesterday_total_likes")).otherwise(0.0))
                .withColumn("daily_trend_score",
                            0.10*F.col("total_views") +
                            0.15*F.col("total_likes") +
                            0.30*F.col("completion_rate") +
                            0.15*F.col("engagement_rate") +
                            0.10*F.col("avg_watch_percentage") +
                            0.10*F.col("views_velocity") +
                            0.10*F.col("likes_velocity")
                            )
                ).select(
        "video_id", "total_views", "total_likes", "total_dislikes", "total_comments", "total_completes", "unique_viewers",
        "avg_watch_percentage", "completion_rate", "engagement_rate", "views_velocity", "likes_velocity", "daily_trend_score"
    )

    return feats_df

def top_N_trends(df:DataFrame, N=50) -> DataFrame:
    window = Window.partitionBy("video_id").orderBy("daily_trend_score")
    df = (df
          .withColumn(
            "rnk",
                      F.row_number().over(window))
          .filter(F.col("rnk") <= N).drop("rnk")
    )

    return df

if __name__ == "__main__":
    spark = utility.create_spark_session("Daily_Recommendation_Job")

    try:
        run_pipeline(spark)

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

    finally:
        spark.stop()