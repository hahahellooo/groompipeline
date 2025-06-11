from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import json
import time
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from kafka.structs import OffsetAndMetadata
from kafka import KafkaConsumer
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

from utils.slack_fail_noti import task_fail_slack_alert

def connect_minio():
    
    s3_hook = S3Hook(
        aws_conn_id='minio',
        region_name='us-east-1'
    )

    return s3_hook

def kafka_consumer(**context):
    try:
        consumer = KafkaConsumer(
            'content-user-events',
            bootstrap_servers='3.38.204.173:9092,16.184.30.150:9092,16.184.31.12:9092',
            group_id='kafka',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
        )

        print("✅ consumer started, waiting for messages....")

        MAX_RETRIES = 3
        RETRY_DELAY_SEC = 10

        for attempt in range(0, MAX_RETRIES+1):
            messages = consumer.poll(timeout_ms=10000)
            if messages:
                print(f"✅ Messages received on attempt {attempt+1}")
                break
            else:
                print(f"🔁 Attempt {attempt+1}: No messages, retrying in {RETRY_DELAY_SEC} seconds...")
                time.sleep(RETRY_DELAY_SEC)
        else:
            raise Exception("❌ No messages received after multiple retries. Broker may be down.")

        try:
            s3_hook = connect_minio()
            print("✅Minio connected")
        except Exception as e:
            print("❌ Failed to connect to MinIO")

        bucket_name = "ml-user-log"

        if not s3_hook.check_for_bucket(bucket_name):
            print(f"No Bucket: ✅{bucket_name} is creating...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            print(f"✅ Bucket '{bucket_name}' created.")
        else:
            print(f"✅ Bucket '{bucket_name}' already exists.")

        # DAG 실행시간으로 파일명 지정
        execution_date = context['execution_date'].astimezone(ZoneInfo("Asia/Seoul"))
        filename = execution_date.strftime("%Y-%m-%d_%H-%M-%S") + ".json"

        with open(filename, "w") as f:
            for tp, message in messages.items():
                for msg in message:
                    try:
                        event = msg.value
                        json.dump(event, f)
                        f.write("\n")
                        print(f"📥 Received: page:{event.get('page')}")
                        consumer.commit(offsets={tp: OffsetAndMetadata(msg.offset + 1, None)})
                    except Exception as e:
                        print(f"❌ Failed to process message: {e}")
                        raise e

        s3_hook.load_file(filename, filename, bucket_name, replace=True)
        print(f"File uploaded to MinIO: {filename}")

    except Exception as e:
        print(f"❌ DAG failed due to : {e}")
        raise e
    
    finally:
        consumer.close()


def check_kafka_broker_health():
    brokers = ["3.38.204.173:9092","16.184.30.150:9092","16.184.31.12:9092"]
    alive_count = 0

    for broker in brokers:
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker, 
                                     request_timeout_ms=5000)
            admin.list_topics()
            alive_count += 1
            print(f"✅ Broker {broker} is alive")
        except KafkaError as e:
            print(f"❌ Broker {broker} failed: {e}")
        finally:
            try:
                admin.close()
            except:
                pass

    if alive_count < 2:
        raise Exception(f"Kafka 브로커가 {alive_count}개만 살아있습니다. 최소 2개 이상 필요합니다.")



with DAG(
    'user_activity_pipeline',
    default_args={
        'depends_on_past':False,
        'retries':2,
        'retry_delay':timedelta(minutes=5),
        'execution_timeout':timedelta(minutes=20),
    },
    description="groomplay",
    start_date=datetime(2025, 5, 19),
    catchup=False,
    schedule_interval='0 1 * * *',
    tags=['user-activity-log', 'ml']
) as dag:
    
    execution_date = "{{ ds }}"
    
    check_kafka_brokers = PythonOperator(
        task_id='check_kafka_broker_health',
        python_callable=check_kafka_broker_health,
        on_failure_callback=task_fail_slack_alert   
    )

    kafka_consumer = PythonOperator(
        task_id='kafka_consumer',
        python_callable=kafka_consumer,
        on_failure_callback=task_fail_slack_alert
    )

    # 업로드 여부 확인 
    check_minio_file = S3KeySensor(
        task_id='check_minio_file',
        bucket_name='ml-user-log',
        bucket_key='*.json',
        wildcard_match=True,
        aws_conn_id='minio',
        poke_interval=5,
        on_failure_callback=task_fail_slack_alert
    )
  
    # 단일 노드 
    spark_etl = SparkSubmitOperator(
        task_id='spark_etl',
        application="/opt/spark/data/userlog_spark.py",
        conn_id='spark',
        jars="/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/spark/jars/postgresql-42.7.4.jar",
        on_failure_callback=task_fail_slack_alert
    )

    # standalone 클러스터
    # spark_etl = SparkSubmitOperator(
    #     task_id='spark_etl',
    #     application="/opt/spark/data/userlog_spark.py",
    #     conn_id='spark_standalone_cluster',
    #     jars="/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/spark/jars/postgresql-42.7.4.jar",
    #     on_failure_callback=task_fail_slack_alert
    # )

    check_kafka_brokers >> kafka_consumer >> check_minio_file >> spark_etl 
