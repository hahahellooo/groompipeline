import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_parquet_path", required=True, help="Source Parquet path in S3 (e.g., s3a://bucket/path/to/data.parquet)")
    parser.add_argument("--iceberg_catalog_name", required=True, help="Name of the Iceberg catalog (e.g., minio_catalog)")
    parser.add_argument("--iceberg_db_name", required=True, help="Name of the Iceberg database (e.g., userlog_db)")
    parser.add_argument("--iceberg_table_name", required=True, help="Name of the target Iceberg table (e.g., user_activity_logs)")
    parser.add_argument("--s3_endpoint", required=True, help="S3 endpoint URL (e.g., http://minio:9000)")
    parser.add_argument("--s3_access_key", required=True, help="S3 access key")
    parser.add_argument("--s3_secret_key", required=True, help="S3 secret key")
    args = parser.parse_args()

    target_iceberg_table = f"{args.iceberg_catalog_name}.{args.iceberg_db_name}.{args.iceberg_table_name}"
    # HadoopCatalog를 사용할 경우 warehouse 경로는 카탈로그 설정 시 지정됩니다.
    # 여기서는 카탈로그 이름에 warehouse 경로가 포함되어 있다고 가정하거나,
    # SparkSession 빌더에서 카탈로그별 warehouse 경로를 명시적으로 설정합니다.
    # 예: spark.sql.catalog.{args.iceberg_catalog_name}.warehouse=s3a://your-iceberg-warehouse/
    # 여기서는 Airflow DAG에서 SparkSubmitOperator의 conf로 전달하는 것을 가정합니다.

    spark = SparkSession.builder \
        .appName(f"ParquetToIceberg_{args.iceberg_db_name}_{args.iceberg_table_name}") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog") \
        .config(f"spark.sql.catalog.{args.iceberg_catalog_name}.warehouse", f"s3a://{args.iceberg_db_name}") \
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

    print(f"✅ SparkSession created. Reading Parquet data from: {args.source_parquet_path}")

    # Parquet 데이터 읽기
    # source_parquet_path가 디렉터리일 수도 있고, 단일 파일일 수도 있습니다.
    # DAG에서 전달되는 XCom 값이 단일 파일 경로이므로, 그대로 사용합니다.
    df = spark.read.parquet(args.source_parquet_path)

    print("✅ Parquet data read successfully. Schema:")
    df.printSchema()

    # timestamp (Long 타입, Unix timestamp milliseconds로 가정) 에서 event_date (DateType) 컬럼 생성
    # 만약 timestamp가 초 단위라면 / 1000 부분은 제거합니다.
    df_with_event_date = df.withColumn("event_date", to_date(from_unixtime(col("timestamp") / 1000)))

    print(f"✅ 'event_date' column added. Writing to Iceberg table: {target_iceberg_table}")
    df_with_event_date.show(5, truncate=False)

    # Iceberg 테이블에 데이터 쓰기 (append 모드, event_date로 파티셔닝)
    # 테이블이 없으면 생성되고, 있으면 데이터가 추가됩니다.
    df_with_event_date.write \
        .format("iceberg") \
        .mode("append") \
        .option("write.wap.enabled", "true") \
        .partitionedBy("event_date") \
        .save(target_iceberg_table)

    print(f"✅ Data successfully appended to Iceberg table: {target_iceberg_table}")

    # (선택사항) 테이블 최적화 (작은 파일 병합 등)
    # spark.sql(f"CALL {args.iceberg_catalog_name}.system.rewrite_data_files(table => '{args.iceberg_db_name}.{args.iceberg_table_name}')").show()
    # print(f"✅ Data files rewritten for table: {target_iceberg_table}")

    spark.stop()
    print("✅ SparkSession stopped.")

if __name__ == "__main__":
    main()
