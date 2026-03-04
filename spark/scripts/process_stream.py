from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

def build_spark_session(
        database: str,
        collection: str
    ) -> SparkConf:
    config = {
        "spark.mongodb.write.connection.uri": "mongodb://spark:spark@mongodb:27017/test?authSource=test",
        "spark.mongodb.write.database": database,
        "spark.mongodb.write.collection": collection,
        "checkpointLocation": "/tmp/checkpoints/mongodb_persons_stream"
    }
    spark_conf = SparkConf()

    for k, v in config.items():
        spark_conf = spark_conf.set(k, v)
    return spark_conf

def main():
    json_schema = StructType([
        StructField("name", StringType()),
        StructField("country_code", StringType()),
        StructField("city", StringType()),
        StructField("street_name", StringType()),
        StructField("postcode", StringType()),
        StructField("email", StringType()),
        StructField("ipv4", StringType()),
        StructField("ipv6", StringType()),
        StructField("user_id", StringType())
    ])

    spark_conf = build_spark_session(database='test', collection='persons')
    spark = \
        SparkSession.builder \
            .master('spark://spark-master:7077') \
            .appName('process_stream') \
            .config(conf=spark_conf) \
            .getOrCreate()

    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'kafka:9092') \
        .option('subscribe', 'persons') \
        .option('startingOffsets', 'earliest') \
        .load()

    df = df \
        .select(from_json(col('value').cast('string'), json_schema).alias('data')) \
        .select('data.*')

    query = df \
        .writeStream \
        .format('mongodb') \
        .outputMode('append') \
        .trigger(processingTime='10 seconds') \
        .option('checkpointLocation', '/tmp/checkpoints/persons_stream') \
        .start()

    query.awaitTermination()

main()
