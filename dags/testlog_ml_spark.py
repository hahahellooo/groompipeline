from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from datetime import timezone, timedelta, datetime
from minio import Minio

spark = SparkSession.builder.getOrCreate()

# CSV 파일을 Spark DataFrame으로 읽기
file_path = 's3a://user-log-ml/'
df = spark.read.csv(file_path, header=True, inferSchema=True)

df_with_scores = (
    df
    .withColumn("click_score", when(col("event_type") == "movie_click", lit(1)).otherwise(lit(0)))
    .withColumn("like_score", when(col("event_type") == "like_click") & (col("liked") == 1, lit(3))
                            .when(col("event_type") == "like_click") & (col('liked' == 0), lit(-3))
                            .otherwise(lit(0)))
    .withColumn("rating", when(col("event_type") == "rating_submit", col("rating")).otherwise(lit(0)))
    .withColumn("review", when(col("event_type") == "review_submit", col("review")).otherwise(lit(0)))
)

# 사실상 click_score만 집계하면 됌
df_scores = df_with_scores.drop('timestamp', 'event_type', 'movie_category', 'page')
scores_agg_df = (
    df_scores
    .groupBy('user_id','movie_id')
    .sum('rating','review','click_score','like_score')
    .withColumnRenamed('sum(click_score)', 'total_movie_click')
    .withColumnRenamed('sum(like_score)', 'total_like')
    .withColumnRenamed('sum(rating)', 'total_rating')
    .withColumnRenamed('sum(review)', 'total_review')
    .withColumn('total_score', col('total_rating') + col('total_review') + col('total_movie_click') + col('total_like'))
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
    url="jdbc:postgresql://localhost:5432/userlog_db",
    table="test",
    mode="overwrite",
    properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
)
