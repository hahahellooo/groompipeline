import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, to_timestamp
from pyspark.sql import functions as F
import csv
from datetime import datetime, timezone, timedelta
from minio import Minio

os.environ['JAVA_HOME']='/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home'

# pyspark, hadoop-aws JAR 경로
spark_jars = "/Users/jeongmieun/app/spark/jars/hadoop-aws-3.3.1.jar,/Users/jeongmieun/app/spark/jars/aws-java-sdk-bundle-1.11.901.jar"

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
file_path = 's3a://user-activity-log/'
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 타임스탬프 컬럼을 'timestamp' 타입으로 변환
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# 결측값 확인 (각 컬럼별 결측값 비율 출력)
missing_data = df.select([count(F.when(col(c).isNull(), c)).alias(c) for c in df.columns])
missing_data.show()

# 각 영화 카테고리별로 가장 인기 있는 활동 (movie_category별 count)
category_activity = df.groupBy("movie_category", "event_type").agg(
    count("event_type").alias("event_count")
).orderBy("event_count", ascending=False)

# 특정 시간대에 발생한 활동 분석
df = df.withColumn("hour", F.hour(col("timestamp")))
hourly_activity = df.groupBy("hour", "event_type").agg(
    count("event_type").alias("event_count")
).orderBy("hour")

# 캠페인별 성과 분석
campaign_performance = df.groupBy("utm_campaign", "utm_source", "utm_medium").agg(
    count("event_type").alias("campaign_click_count")
).orderBy("campaign_click_count", ascending=False)


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

bucket_name = "user-activity-output"
output_base_path = f"s3a://{bucket_name}/"

# 디렉터리가 존재할 경우 덮어쓰도록 설정 (optional)
write_mode = "overwrite"

# 각 분석 결과를 Parquet 파일로 저장
category_activity.write.mode(write_mode).parquet(output_base_path + f"category_activity_{filename}.parquet")
hourly_activity.write.mode(write_mode).parquet(output_base_path + f"hourly_activity_{filename}.parquet")
campaign_performance.write.mode(write_mode).parquet(output_base_path + f"campaign_performance_{filename}.parquet")
