FROM apache/airflow:2.10.1-python3.10

USER airflow

RUN pip install --no-cache-dir \
    kafka-python \
    minio \
    pyspark \
    boto3 \
    faker
    
