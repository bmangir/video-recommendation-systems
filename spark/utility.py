import json

from pyspark.sql import SparkSession, DataFrame

from config import MONGO_URI, PG_DB_URL, PG_USER, PG_PW


def create_spark_session(app_name, memory_intensive: bool = False) -> SparkSession:
    """
    Args:
        app_name: Application name
        memory_intensive: If True, applies memory optimization settings for ML workloads
    """
    try:
        builder = SparkSession \
            .builder \
            .appName(app_name) \
            .config("spark.jars.packages",
                    "org.postgresql:postgresql:42.6.0,"
                    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
            .config("spark.mongodb.read.connection.uri", MONGO_URI) \
            .config("spark.mongodb.write.connection.uri", MONGO_URI) \
            .config("spark.sql.shuffle.partitions", "50") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.mongodb.read.readPreference.name", "secondaryPreferred")
        
        # Memory optimization for ML workloads (BERT, etc.)
        if memory_intensive:
            builder = builder \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.python.worker.memory", "2g") \
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        spark = builder.getOrCreate()

    except Exception as e:
        raise RuntimeError("SparkSession oluşturulamadı") from e

    return spark

def read_coll(
        spark,
        db_name: str,
        coll_name: str,
        filter: dict | str | None = None,
        partitions: int = 10,) -> DataFrame:

    if filter:
        if isinstance(filter, str):
            filter = json.loads(filter)
        pipeline = [{"$match": filter}]
    else:
        pipeline = []

    reader = (
        spark.read
        .format("mongodb")
        .option("database", db_name)
        .option("collection", coll_name)
        .option("partitioner", "MongoSplitVectorPartitioner")
        .option("partitionerOptions.partitionSizeMB", "64")
    )

    if pipeline:
        reader = reader.option("pipeline", json.dumps(pipeline))

    return reader.load()


def read_table(
        spark,
        table: str,
        where_clause: str | None = None,
        partition_column: str | None = None,
        lower_bound: int | None = None,
        upper_bound: int | None = None,
        num_partitions: int = 10) -> DataFrame:

    base_reader = (
        spark.read
        .format("jdbc")
        .option("url", PG_DB_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PW)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
    )

    use_partitioning = False

    # If partition column is not None
    if partition_column:
        if lower_bound is None or upper_bound is None:
            try:
                bounds_query = f"""
                    (SELECT
                        MIN({partition_column}) AS min_val,
                        MAX({partition_column}) AS max_val
                     FROM {table}
                    ) AS bounds
                """

                bounds = (
                    spark.read
                    .format("jdbc")
                    .option("url", PG_DB_URL)
                    .option("dbtable", bounds_query)
                    .option("user", PG_USER)
                    .option("password", PG_PW)
                    .option("driver", "org.postgresql.Driver")
                    .load()
                    .first()
                )

                if bounds and bounds.min_val is not None and bounds.max_val is not None:
                    lower_bound = int(bounds.min_val)
                    upper_bound = int(bounds.max_val)
                    use_partitioning = lower_bound < upper_bound
            except Exception:
                use_partitioning = False
        else:
            use_partitioning = lower_bound < upper_bound

    reader = base_reader

    if use_partitioning:
        reader = (
            reader
            .option("partitionColumn", partition_column)
            .option("lowerBound", lower_bound)
            .option("upperBound", upper_bound)
            .option("numPartitions", num_partitions)
        )

    df = reader.load()

    if where_clause:
        df = df.where(where_clause)

    return df

def save_to_pg(
        df: DataFrame,
        table: str,
        num_partitions: int = 8,
        batch_size: int = 10000,
):
    # stringtype=unspecified allow PostgresSQL to cast string to JSONB automatically
    jdbc_url_with_options = f"{PG_DB_URL}?stringtype=unspecified"
    
    (
        df.repartition(num_partitions)
        .write
        .format("jdbc")
        .option("url", jdbc_url_with_options)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PW)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", batch_size)
        .option("isolationLevel", "READ_COMMITTED")
        .mode("append")
        .save()
    )

def save_to_mongodb(
        df: DataFrame,
        db_name: str,
        coll_name: str,
        num_partitions: int = 8,
        write_concern_w: str = "1",
):
    (
        df.repartition(num_partitions)
        .write
        .format("mongodb")
        .option("database", db_name)
        .option("collection", coll_name)
        .option("writeConcern.w", write_concern_w)
        .mode("append")
        .save()
    )