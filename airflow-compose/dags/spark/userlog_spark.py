from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from datetime import timezone, timedelta, datetime
from minio import Minio
from pyspark.sql.functions import sum as _sum, col

# 1. SparkSession 생성
spark = SparkSession.builder \
    .appName("ml") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://54.180.166.228:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print("✅ SparkSession started")

# 2. 데이터 로딩 및 전처리
df = spark.read.option("multiline", "false").json('s3a://ml-user-log/')
df = df.drop('timestamp', 'page', 'contentCategory')

# 3. 스코어 컬럼 추가 및 컬럼명 정리
df = (
    df
    .withColumnRenamed("userId", "user_id")
    .withColumnRenamed("videoId", "video_id")
    .withColumn("like_score", when((col("eventType") == "like_click") & (col("liked") == 'true'), lit(3)).otherwise(lit(0)))
    .withColumn("review_score", when((col("eventType") == "review_write") & col("review").isNotNull(), lit(1)).otherwise(lit(0)))
    .withColumn("rating_score", when((col("eventType") == "rating_submit") & col("rating").isNotNull(), col("rating").cast("int")).otherwise(lit(0)))
    .withColumn("click_score", when(col("eventType") == "content_click", lit(1)).otherwise(lit(0)))
    .withColumn("total_score", col("like_score") + col("review_score") + col("rating_score") + col("click_score"))
)

# 4. Aggregation
df_agg = (
    df.groupBy("user_id", "video_id")
    .agg(
        _sum("like_score").alias("like_score"),
        _sum("review_score").alias("review_score"),
        _sum("rating_score").alias("rating_score"),
        _sum("click_score").alias("click_score"),
        _sum("total_score").alias("total_score")
    )
    .orderBy("user_id", "video_id")
)

# 5. 필요한 데이터만 추출 (모델용)
df_score = df_agg.select("user_id", "video_id", "total_score")

# 6. 시간 기반 파일명 생성
kst = timezone(timedelta(hours=9))
filename = datetime.now(kst).strftime("%Y-%m-%d")

# 7. MinIO 연결 및 버킷 체크
client = Minio("54.180.166.228:9000", "minioadmin", "minioadmin", secure=False)
buckets = ["ml-learning-data", "ml-backup-data"]

for bucket in buckets:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"✅ Created bucket: {bucket}")
    else:
        print(f"ℹ️ Bucket exists: {bucket}")

# 8. 저장
df_agg.write.mode("overwrite").parquet(f"s3a://ml-backup-data/{filename}.parquet")
df_score.write.mode("overwrite").parquet(f"s3a://ml-learning-data/{filename}.parquet")
