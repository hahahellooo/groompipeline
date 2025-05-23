from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from datetime import timezone, timedelta, datetime
from minio import Minio

# spark_jars="/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/spark/jars/postgresql-42.7.4.jar"

spark = SparkSession.builder \
    .appName("pipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://54.180.166.228:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()
    # .config("spark.jars", spark_jars) \
    
    
# CSV 파일을 Spark DataFrame으로 읽기
file_path = 's3a://user-log-ml/'
df = spark.read.csv(file_path, header=True, inferSchema=True)

df_with_scores = (
    df
    .withColumn("click_score", when(col("event_type") == "movie_click", lit(1)).otherwise(lit(0)))
    .withColumn("like_score", 
                when((col("event_type") == "like_click") & (col('liked') == 1), lit(3))
                .when((col("event_type") == "like_click") & (col('liked') == 0), lit(-3))
                .otherwise(lit(0)))
    .withColumn("rating", when(col("event_type") == "rating_submit", col("rating")).otherwise(lit(0)))
    .withColumn("review", when(col("event_type") == "review_submit", col("review")).otherwise(lit(0)))
)

# 사실상 click_score만 집계하면 됌
df_scores = df_with_scores.drop('timestamp', 'event_type', 'movie_category', 'page')
scores_df = (
    df_scores
    .groupBy('user_id','movie_id')
    .sum('rating','review','click_score','like_score')
    .withColumnRenamed('sum(click_score)', 'click_score')
    .withColumnRenamed('sum(like_score)', 'like_score')
    .withColumnRenamed('sum(rating)', 'rating_score')
    .withColumnRenamed('sum(review)', 'review_score')
    .withColumn('total_score', col('rating_score') + col('review_score') + col('click_score') + col('like_score'))
    .orderBy(col('user_id').asc())
    .orderBy(col('movie_id').asc())
)

scores_df.show()

scores_agg_df = (
    scores_df
    .select('user_id', 'movie_id', 'total_score')
)

scores_agg_df.show()

# 한국 시간(KST)으로 현재 시간 가져오기
kst = timezone(timedelta(hours=9))  # KST는 UTC +9 시간대
current_time_kst = datetime.now(kst)
filename = current_time_kst.strftime("%Y-%m-%d")

client = Minio(
    "54.180.166.228:9000",  # MinIO 서버의 주소
    access_key='minioadmin',  # 액세스 키
    secret_key='minioadmin',  # 시크릿 키
    secure=False  # HTTPS를 사용하지 않는 경우
)

bucket_name = ["ml-data", "ml-backup"]

for bucket in bucket_name:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"✅Bucket: {bucket} is created")
    else:
        print(f"ℹ️  Bucket '{bucket}' already exists.")


backup_path = f"s3a://ml-backup/"
data_path = f"s3a://ml-data/"

############################### postgres로 보내는 데이터는 append 고려###########################
# 디렉터리가 존재할 경우 덮어쓰도록 설정 
write_mode = "overwrite"

scores_df.write.mode(write_mode).parquet(backup_path + f"{filename}.parquet")
scores_agg_df.write.mode(write_mode).parquet(data_path + f"{filename}.parquet")

# test - write postgresSQL 
scores_agg_df.write.jdbc(
    url="jdbc:postgresql://3.39.148.207:5432/userlog_db?ssl=false",
    table="test",
    mode="overwrite",
    properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
)
