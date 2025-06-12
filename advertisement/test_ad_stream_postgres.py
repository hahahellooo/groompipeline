from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("KafkaToPostgresBatchJob") \
    .getOrCreate()

# Kafka 메시지 스키마 정의
schema = StructType() \
    .add("ad_id", StringType()) \
    .add("event_type", StringType()) \
    .add("cost", IntegerType()) \
    .add("timestamp", TimestampType()) \

# Kafka에서 데이터를 batch로 읽음 (1회 실행)
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "3.34.30.146:9092,3.36.10.141:9092,43.203.117.45:9092") \
    .option("subscribe", "ad-events") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Kafka value 컬럼은 binary이므로 문자열로 변환 후 JSON 파싱
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# PostgreSQL로 적재
parsed_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/ad_log_db") \
    .option("dbtable", "ad_billing_logs") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
