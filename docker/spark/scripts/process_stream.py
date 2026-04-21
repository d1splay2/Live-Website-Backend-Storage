from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

class MongoDBConfig:
    def __init__(self, database: str, collection: str):
        self.database = database
        self.collection = collection
        self.config = {
            f"spark.mongodb.write.connection.uri": f"mongodb://spark:spark@mongodb:27017/{database}?authSource={database}",
            f"spark.mongodb.write.database": database,
            f"spark.mongodb.write.collection": collection,
            "spark.driver.host": "spark-driver",
            "spark.driver.port": "7078",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.blickManager.port": "7079"
        }

    def get_spark_conf(self) -> SparkConf:
        """Set mandatory configuration to work"""
        spark_conf = SparkConf()
        for key, value in self.config.items():
            spark_conf.set(key, value)
        return spark_conf


class KafkaStreamProcessor:
    """
    Process specified kafka topic in continuous stream

    Args:
        topic: Where data is being taken
        database: Name of DB in MongoDB
        collectin: Name of collection in MongoDB
        json_schema: Structure now data should be read and written
        spark_master_domain: Where spark master reside
        spark_master_ip: str, IP of a spark master
        kafka_domain: Where kafka reside
        kafka_master_ip: IP of kafka
    """
    def __init__(
        self,
        topic: str,
        database: str,
        collection: str,
        json_schema: StructType,
        spark_master_name: str = "spark-master",
        spark_master_ip: str = "7077",
        kafka_master_name: str = "kafka",
        kafka_master_ip: str = "9092"
    ):
        self.topic = topic
        self.database = database
        self.collection = collection
        self.json_schema = json_schema
        self.spark_master_name = spark_master_name
        self.spark_master_ip = spark_master_ip
        self.kafka_master_name = kafka_master_name
        self.kafka_master_ip = kafka_master_ip

    def build_spark_session(self) -> SparkSession:
        """Build session with all required variables"""
        spark_conf = MongoDBConfig(
                                self.database,
                                self.collection) \
                                .get_spark_conf()

        return SparkSession.builder \
            .master(f"spark://{self.spark_master_name}:{self.spark_master_ip}") \
            .appName(f'from "{self.topic}" topic to {self.database}.{self.collection}') \
            .config(conf=spark_conf) \
            .getOrCreate()

    def process_stream(self, spark: SparkSession):
        # Start reading Kafka topic
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f"{self.kafka_master_name}:{self.kafka_master_ip}") \
            .option("subscribe", self.topic) \
            .load()

        # Parse JSON
        df = df \
            .select(from_json(col("value").cast("string"), self.json_schema).alias("data")) \
            .select("data.*")

        # Write to MongoDB
        query = df \
            .writeStream \
            .format("mongodb") \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .option("checkpointLocation", f"/tmp/checkpoints/{self.database}/{self.collection}") \
            .start()

        # Run indefinitely
        query.awaitTermination()



if __name__ == "__main__":
    json_schema = StructType([
        StructField("name", StringType()),
        StructField("country_code", StringType()),
        StructField("city", StringType()),
        StructField("street_name", StringType()),
        StructField("postcode", StringType()),
        StructField("email", StringType()),
        StructField("ipv4", StringType()),
        StructField("ipv6", StringType()),
        StructField("user_id", IntegerType())
    ])

    processor = KafkaStreamProcessor(
        topic="persons",
        database="data",
        collection="persons",
        json_schema=json_schema
    )

    spark = processor.build_spark_session()
    processor.process_stream(spark)
