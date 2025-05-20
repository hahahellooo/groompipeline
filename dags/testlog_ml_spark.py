from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from pyspark.sql import functions as F
from datetime import timezone, timedelta, datetime
import os
from minio import Minio

os.environ['JAVA_HOME']='/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home'

# pyspark, hadoop-aws JAR 경로
#spark_jars = "/Users/jeongmieun/app/spark/jars/hadoop-aws-3.2.0.jar,/Users/jeongmieun/app/spark/jars/aws-java-sdk-bundle-1.11.375.jar"
spark_jars = "/Users/jeongmieun/app/spark/jars/hadoop-aws-3.3.1.jar,/Users/jeongmieun/app/spark/jars/aws-java-sdk-bundle-1.11.901.jar,/Users/jeongmieun/app/spark/jars/postgresql-42.7.4.jar"

spark = SparkSession.builder \
    .appName("try") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "") \
    .config("spark.jars", spark_jars) \
    .getOrCreate()


# CSV 파일을 Spark DataFrame으로 읽기
file_path = 's3a://user-log-ml/'
df = spark.read.csv(file_path, header=True, inferSchema=True)

df_with_scores = (
    df.withColumn("click_score", when(col("event_type") == "movie_click", lit(1)).otherwise(lit(0)))
      .withColumn("like_score", when(col("event_type") == "like_click", lit(3)).otherwise(lit(0)))
      .withColumn("rating", when(col("event_type") == "rating_submit", col("rating")).otherwise(lit(0)))
      .withColumn("review", when(col("event_type") == "review_submit", col("review")).otherwise(lit(0)))
)

# 사실상 click_score만 집계하면 됌
df_scores = df_with_scores.drop('timestamp', 'event_type', 'movie_category', 'page')
scores_agg_df = (
    df_scores
    .groupBy('user_id','movie_id')
    .sum('rating','review','click_score','like_score')
    .withColumnRenamed('sum(click_score)','total_movie_click')
    .orderBy(col('user_id').asc())
)
scores_agg_df.show()

# 한국 시간(KST)으로 현재 시간 가져오기
kst = timezone(timedelta(hours=9))  # KST는 UTC +9 시간대
current_time_kst = datetime.now(kst)
filename = current_time_kst.strftime("%Y-%m-%d")

client = Minio(
    "localhost:9000",  # MinIO 서버의 주소
    access_key='minioadmin',  # 액세스 키
    secret_key='minioadmin',  # 시크릿 키
    secure=False  # HTTPS를 사용하지 않는 경우
)

bucket_name = "ml-data"
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"✅Bucket: {bucket_name} is created")


output_base_path = f"s3a://{bucket_name}/"

# 디렉터리가 존재할 경우 덮어쓰도록 설정 (optional)
write_mode = "overwrite"

scores_agg_df.write.mode(write_mode).parquet(output_base_path + f"{filename}.parquet")

# test - write postgresSQL 
scores_agg_df.write.jdbc(
    url="jdbc:postgresql://localhost:5434/log_db",
    table="log_tb",
    mode="overwrite",
    properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
)
