from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer, KafkaConsumer
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import json
import csv
import random
import time
from io import StringIO, BytesIO
from datetime import datetime, timezone, timedelta
from kafka.structs import OffsetAndMetadata
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from kafka import KafkaAdminClient
from kafka.errors import KafkaError

def generate_log():
    # 한국 시간대로 설정
    kst = timezone(timedelta(hours=9))
    # 샘플 카테고리
    categories = ["Action", "Drama", "Comedy", "Sci-Fi", "Horror", "Romance"]
    
    # key=user_id로 활용하는데 Int는 encoding이 안됌
    user_id = str(random.randint(100, 150))
    movie_id = f"M{random.randint(1, 30):03d}"
    timestamp = datetime.now(kst).isoformat()
    event_type = random.choice(["movie_click", "like_click", "rating_submit", "review_submit"])
    movie_category = random.choice(categories)

    base = {
        "user_id": user_id,
        "movie_id": movie_id,
        "timestamp": timestamp,
        "event_type": event_type,
        "movie_category": movie_category
    }

    if event_type == "movie_click":
        base["page"] = "main"
    elif event_type == "like_click":
        base["page"] = "movie_detail"
        base["liked"] = random.randint(0, 1)
    elif event_type == "rating_submit":
        base["page"] = "movie_detail"
        base["rating"] = random.randint(0, 5)
    elif event_type == "review_submit":
        base["page"] = "movie_detail"
        base["review"] = random.randint(0, 1)

    return base

def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='3.37.147.123:9092,3.36.188.73:9092,54.180.180.120:9092',
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=2,
        acks='all'
    )

    for i in range(1000):
        event = generate_log()
        user_id = event['user_id']
        producer.send('userlog', key=user_id,value=event)
        print(f"💌message {i+1} sent: user_id={user_id}")
        time.sleep(0.05)

    producer.flush()
    print("✅All messages sent")

def connect_minio():
    
    s3_hook = S3Hook(
        aws_conn_id='minio',
        region_name='us-east-1'
    )

    return s3_hook

def kafka_consumer(**context):
    try:
        consumer = KafkaConsumer(
            'userlog',
            bootstrap_servers='3.37.147.123:9092,3.36.188.73:9092,54.180.180.120:9092',
            group_id='monst',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000
        )

        print("✅ consumer started, waiting for messages....")

        message = consumer.poll(timeout_ms=5000)

        MAX_RETRIES = 5
        RETRY_DELAY_SEC = 10

        for attempt in range(MAX_RETRIES):
            message = consumer.poll(timeout_ms=5000)
            if message:
                print(f"✅ Messages received on attempt {attempt+1}")
                break
            else:
                print(f"🔁 Attempt {attempt+1}: No messages, retrying in {RETRY_DELAY_SEC} seconds...")
                time.sleep(RETRY_DELAY_SEC)
        else:
            raise Exception("❌ No messages received after multiple retries. Broker may be down.")


        csv_file = StringIO()
        fieldnames = ["user_id", "movie_id", "timestamp", "event_type", "movie_category", "page", "rating", "review", "liked"]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for tp,messages in message.items():
            for msg in messages:
                try:
                    event = msg.value
                    writer.writerow({key: event.get(key, None) for key in fieldnames})
                    print(f"📥 Received: page:{event.get('page')}")
                    # tp: TopicPartition 객체 / 다음 offset부터 읽는 걸로 설정
                    consumer.commit(offsets={tp: OffsetAndMetadata(msg.offset + 1, None)})
                except Exception as e:
                    print(f"❌ Failed to process message: {e}")
                    raise e      
        try:
            s3_hook = connect_minio()
            print("✅Minio connected")
        except Exception as e:
            print("❌ Failed to connect to MinIO")

        # csv를 minio에 업로드(메모리상에 기록된 csv를)
        csv_file.seek(0) # 파일 포인터를 처음으로 되돌림

        # StringIO 객체를 바이트형으로 변환하고, 이를 BytesIO 객체로 감쌈
        csv_data = csv_file.getvalue().encode('utf-8')  # 문자열을 바이트로 변환        
        csv_stream = BytesIO(csv_data)  # 바이트 데이터를 BytesIO 객체로 변환

        bucket_name = "user-log-ml"

        if not s3_hook.check_for_bucket(bucket_name):
            print(f"No Bucket: ✅{bucket_name} is creating...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            print(f"✅ Bucket '{bucket_name}' created.")
        else:
            print(f"✅ Bucket '{bucket_name}' already exists.")

    
        # DAG 실행시간으로 파일명 지정
        execution_date = context['execution_date'].astimezone(timezone(timedelta(hours=9)))
        filename = execution_date.strftime("%Y-%m-%d_%H-%M-%S") + ".csv"

        s3_hook.load_file_obj(csv_stream, filename, bucket_name, replace=True)
        print(f"✅ File uploaded to MinIO: {filename}")
    
    except Exception as e:
        print(f"❌ DAG failed due to : {e}")
        raise e
    
    finally:
        consumer.close()


def task_fail_slack_alert(context):
    kst = timezone(timedelta(hours=9))
    utc_time = context['execution_date']
    kst_time = utc_time.astimezone(kst)
    return SlackWebhookOperator(
        task_id=f"notify_failure_{context['task_instance'].task_id}",  # 동적으로 유일하게
        slack_webhook_conn_id="slack",
        message=f"""
            :red_circle: Task Failed!
            *Task*: {context['task_instance'].task_id}
            *DAG*: {context['dag'].dag_id}
            *Execution Time*: {kst_time.strftime('%Y-%m-%d %H:%M:%S')}
        """,
        username="airflow"
    ).execute(context=context)

def check_kafka_broker_health():
    brokers = ["3.37.147.123:9092", "3.36.188.73:9092", "54.180.180.120:9092"]
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
    'kafka_to_minio_to_spark',
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
    
    kafka_producer = PythonOperator(
        task_id='kafka_producer',
        python_callable=kafka_producer,
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
        bucket_name='user-log-ml',
        bucket_key='*.csv',
        wildcard_match=True,
        aws_conn_id='minio',
        poke_interval=5,
        on_failure_callback=task_fail_slack_alert
    )
  
    spark_etl = SparkSubmitOperator(
        task_id='spark_etl',
        application="/opt/spark/testlog_ml_spark.py",
        conn_id='spark',
        jars="/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar,/opt/spark/jars/postgresql-42.7.4.jar",
        on_failure_callback=task_fail_slack_alert
    )



    check_kafka_brokers >> kafka_producer >> kafka_consumer >> check_minio_file >> spark_etl 
    
