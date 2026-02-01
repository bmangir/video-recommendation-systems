from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, FloatType, StructType, StructField
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import time
import json
from datetime import datetime, timedelta

import spark.utility as utility
import config

"""
Implicit Rating Formula:
    rating = 1.0 * view_count
           + 2.0 * watch_ratio (if > 50%)
           + 3.0 * is_completed
           + 3.5 * rewatch_count
           + 4.0 * is_liked
           - 4.0 * is_disliked
           + 5.0 * has_commented
"""

MODEL_VERSION = f"als_v1.0_{datetime.now().strftime('%Y%m%d')}"
NUM_RECOMMENDATIONS = 20  # Top N recommendations per user
TRAIN_TEST_SPLIT = 0.8    # Train: 80%, Test:20%
NEGATIVE_SAMPLES_PER_USER = 50

# ALS Hyperparameters
ALS_PARAMS = {
    "rank": 10,                     # Number of latent factors
    "maxIter": 10,                  # Maximum iterations
    "regParam": 0.1,                # Regularization parameter
    "implicitPrefs": True,          # Implicit feedback mode
    "coldStartStrategy": "drop",    # Handle cold start
    "seed": 44
}


def run_pipeline(spark: SparkSession, save_model: bool = True):
    """
    Main pipeline to train ALS model and generate recommendations.
    """
    
    start_time = time.time()
    
    # Fetch data
    videos_df, comments_df, likes_df, watch_history_df = fetch_data(spark)
    
    # Prepare features (calculating implicit ratings)
    rating_df = prepare_features(videos_df, comments_df, likes_df, watch_history_df)
    rating_df.cache()
    
    unique_users = rating_df.select("user_id").distinct().count()
    unique_videos = rating_df.select("video_id").distinct().count()

    # Train/Test Split
    train_df, test_df = rating_df.randomSplit([TRAIN_TEST_SPLIT, 1 - TRAIN_TEST_SPLIT], seed=42)
    train_df.cache()
    test_df.cache()
    
    # Train model
    model, training_time = train_model(train_df)
    
    # Evaluate metrics
    metrics = evaluate_model(model, test_df)
    
    # Generate recommendations
    recommendations_df = generate_recommendations(
        spark, model, unique_users, unique_videos, training_time
    )
    
    # Save
    save_recommendations(recommendations_df)

    rating_df.unpersist()
    train_df.unpersist()
    test_df.unpersist()
    
    # Save model to local
    if save_model:
        model_path = f"{config.MODEL_PATH}/als_model_{MODEL_VERSION}"
        save_model_to_disk(model, model_path)
        print(f"\n   Model saved to: {model_path}")
    
    total_time = time.time() - start_time
    print(f"PIPELINE COMPLETED in {total_time:.2f} seconds")
    
    return model, metrics


def fetch_data(spark: SparkSession):
    """Fetch required tables from PostgresSQL."""
    
    videos_df = utility.read_table(
        spark, 
        table=f"{config.CORE_SCHEMA}.videos",
        partition_column="id"
    )
    
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

    return videos_df, comments_df, likes_df, watch_history_df

def prepare_features(
            videos_df: DataFrame,
            comments_df: DataFrame,
            likes_df: DataFrame,
            watch_history_df: DataFrame) -> DataFrame:
    """
    Prepare features and calculate implicit ratings for each user-video pair.
    """
    
    # Comment features: has_commented flag (0 or 1)
    comments_feats = (
        comments_df
        .filter(F.col("status") == "active")
        .select("user_id", "video_id")
        .distinct()
        .withColumn("has_commented", F.lit(1))
    )

    # Like features: is_liked, is_disliked
    likes_feats = (
        likes_df
        .select("user_id", "video_id", "like_type")
        .dropDuplicates(["user_id", "video_id"])
        .withColumn("is_liked", (F.col("like_type") == 1).cast("int"))
        .withColumn("is_disliked", (F.col("like_type") == -1).cast("int"))
        .drop("like_type")
    )

    # Watch features: view_count, watch_ratio, is_completed, rewatch_count
    watch_feats = (
        watch_history_df
        .select(
            "user_id",
            "video_id",
            F.col("watch_count").alias("view_count"),
            (F.col("watch_percentage") / 100.0).alias("watch_ratio"),
            F.col("is_completed").cast("int").alias("is_completed"),
            "last_watched_at"
        )
        .withColumn(
            "rewatch_count",
            F.when(F.col("view_count") > 1, F.col("view_count") - 1).otherwise(0)
        )
    )

    watch_feats.cache()
    likes_feats.cache()
    comments_feats.cache()

    # Get all positive interactions (user interacted with video)
    positives = (
        watch_feats.select("user_id", "video_id")
        .union(likes_feats.select("user_id", "video_id"))
        .union(comments_feats.select("user_id", "video_id"))
        .distinct()
    )

    # Generate negative samples (videos that user hasn't interacted with)
    candidate_videos = videos_df.select(F.col("id").alias("video_id"))
    
    negatives = (
        positives
        .select("user_id")
        .distinct()
        .crossJoin(candidate_videos)
        .join(positives, on=["user_id", "video_id"], how="left_anti")
        .withColumn("rand", F.rand(seed=42))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy("rand")
            )
        )
        .filter(f"rn <= {NEGATIVE_SAMPLES_PER_USER}")
        .drop("rand", "rn")
    )

    # Union
    base_pairs = positives.union(negatives).distinct()

    features_df = (
        base_pairs
        .join(watch_feats.drop("last_watched_at"), ["user_id", "video_id"], "left")
        .join(likes_feats, ["user_id", "video_id"], "left")
        .join(comments_feats, ["user_id", "video_id"], "left")
        .fillna({
            "view_count": 0,
            "watch_ratio": 0.0,
            "is_completed": 0,
            "rewatch_count": 0,
            "is_liked": 0,
            "is_disliked": 0,
            "has_commented": 0
        })
    )

    # Calculate implicit rating
    rating_df = (
        features_df
        .withColumn(
            "rating",
            (
                1.0 * F.col("view_count") +
                2.0 * F.col("watch_ratio") +
                3.0 * F.col("is_completed") +
                3.5 * F.col("rewatch_count") +
                4.0 * F.col("is_liked") -
                4.0 * F.col("is_disliked") +
                5.0 * F.col("has_commented")
            ).cast("float")
        )
        .select("user_id", "video_id", "rating")
    )

    watch_feats.unpersist()
    likes_feats.unpersist()
    comments_feats.unpersist()

    return rating_df

def train_model(train_df: DataFrame) -> tuple[ALSModel, float]:
    """
    Train ALS model using PySpark MLlib.
    
    Returns:
        model: Trained ALSModel
        training_time: Time taken to train in seconds
    """
    
    als = ALS(
        rank=ALS_PARAMS["rank"],
        maxIter=ALS_PARAMS["maxIter"],
        regParam=ALS_PARAMS["regParam"],
        implicitPrefs=ALS_PARAMS["implicitPrefs"],
        coldStartStrategy=ALS_PARAMS["coldStartStrategy"],
        seed=ALS_PARAMS["seed"],
        userCol="user_id",
        itemCol="video_id",
        ratingCol="rating"
    )
    
    start_time = time.time()
    model = als.fit(train_df)
    training_time = time.time() - start_time
    
    return model, training_time


def train_with_cross_validation(
    train_df: DataFrame, 
    num_folds: int = 3) -> tuple[ALSModel, dict]:
    """
    Train ALS model with cross-validation for hyperparameter tuning.
    
    Returns:
        best_model: Best ALSModel from cross-validation
        best_params: Best hyperparameters
    """
    
    als = ALS(
        implicitPrefs=True,
        coldStartStrategy="drop",
        userCol="user_id",
        itemCol="video_id",
        ratingCol="rating"
    )
    
    # Parameter grid for tuning
    param_grid = (
        ParamGridBuilder()
        .addGrid(als.rank, [5, 10, 15])
        .addGrid(als.maxIter, [5, 10])
        .addGrid(als.regParam, [0.01, 0.1, 0.5])
        .build()
    )
    
    # Evaluator
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    # Cross-validator
    cv = CrossValidator(
        estimator=als,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=num_folds,
        parallelism=4
    )
    
    cv_model = cv.fit(train_df)
    best_model = cv_model.bestModel
    
    best_params = {
        "rank": best_model.rank,
        "maxIter": best_model._java_obj.parent().getMaxIter(),
        "regParam": best_model._java_obj.parent().getRegParam()
    }
    
    return best_model, best_params


def evaluate_model(model: ALSModel, test_df: DataFrame) -> dict:
    """
    Evaluate the trained model on test data.
    
    Metrics:
        - RMSE: Root Mean Square Error
        - MAE: Mean Absolute Error
    """
    
    predictions = model.transform(test_df)
    
    # Filter out NaN predictions (cold start)
    predictions = predictions.filter(F.col("prediction").isNotNull())

    rmse_evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = rmse_evaluator.evaluate(predictions)

    mae_evaluator = RegressionEvaluator(
        metricName="mae",
        labelCol="rating",
        predictionCol="prediction"
    )
    mae = mae_evaluator.evaluate(predictions)
    
    return {"rmse": rmse, "mae": mae}


def calculate_ranking_metrics(
    model: ALSModel, 
    test_df: DataFrame, 
    k: int = 10) -> dict:
    """
    Calculate ranking-based metrics for recommendation evaluation.
    
    Metrics:
        - Precision@K: Proportion of recommended items that are relevant
        - Recall@K: Proportion of relevant items that are recommended
        - NDCG@K: Normalized Discounted Cumulative Gain
    """
    
    # Get actual positives from test set (items with high rating)
    actual_positives = (
        test_df
        .filter(F.col("rating") > 3.0)  # Threshold for positive
        .groupBy("user_id")
        .agg(F.collect_set("video_id").alias("actual_items"))
    )
    
    # Get top-K recommendations per user
    recommendations = model.recommendForAllUsers(k)
    recommended_items = (
        recommendations
        .select(
            "user_id",
            F.expr("transform(recommendations, x -> x.video_id)").alias("recommended_items")
        )
    )
    
    # Join and calculate metrics
    metrics_df = (
        actual_positives
        .join(recommended_items, "user_id", "inner")
        .withColumn(
            "hits",
            F.size(F.array_intersect("actual_items", "recommended_items"))
        )
        .withColumn(
            "precision_at_k",
            F.col("hits") / k
        )
        .withColumn(
            "recall_at_k",
            F.col("hits") / F.size("actual_items")
        )
    )
    
    avg_metrics = metrics_df.agg(
        F.avg("precision_at_k").alias("avg_precision"),
        F.avg("recall_at_k").alias("avg_recall")
    ).collect()[0]
    
    return {
        f"precision@{k}": avg_metrics["avg_precision"] or 0.0,
        f"recall@{k}": avg_metrics["avg_recall"] or 0.0
    }

def generate_recommendations(
    spark: SparkSession,
    model: ALSModel,
    num_users: int,
    num_videos: int,
    training_time: float) -> DataFrame:
    """
    Generate top-N recommendations for all users.
    
    Returns DataFrame with columns matching als_recommendations table schema.
    """
    
    start_time = time.time()

    # Generate recommendations
    raw_recommendations = model.recommendForAllUsers(NUM_RECOMMENDATIONS)
    
    # Transform to match with table schema
    recommendations_df = (
        raw_recommendations
        .select(
            F.col("user_id"),
            F.expr("transform(recommendations, x -> x.video_id)")
                .cast(ArrayType(IntegerType()))
                .alias("recommended_video_ids"),
            F.expr("transform(recommendations, x -> CAST(x.rating AS DECIMAL(6,4)))")
                .alias("scores")
        )
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("model_params", F.lit(json.dumps(ALS_PARAMS)))
        .withColumn("candidates_evaluated", F.lit(num_videos))
        .withColumn("generation_time_ms", F.lit(int(training_time * 1000)))
        .withColumn("generated_at", F.current_timestamp())
        .withColumn("expires_at", F.lit(datetime.now() + timedelta(days=1)))
        .withColumn("is_active", F.lit(True))
    )
    
    generation_time = time.time() - start_time
    print(f"Generated recommendations for {num_users} users in {generation_time:.2f}s")
    
    return recommendations_df

def save_recommendations(recommendations_df: DataFrame):
    """Save recommendations to PostgresSQL als_recommendations table."""
    
    utility.save_to_pg(
        recommendations_df,
        table=f"{config.BATCH_OUTPUT_SCHEMA}.als_recommendations"
    )


def save_model_to_disk(model: ALSModel, path: str):
    """Save trained ALS model to disk for later use."""
    model.save(path)


def load_model_from_disk(spark: SparkSession, path: str) -> ALSModel:
    """Load a previously saved ALS model."""
    return ALSModel.load(path)


if __name__ == "__main__":
    spark = utility.create_spark_session("ALS_Recommendation_Job")
    
    try:
        model, metrics = run_pipeline(spark, save_model=False)
        for metric, value in metrics.items():
            print(f"  {metric}: {value:.4f}")
            
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise
        
    finally:
        spark.stop()