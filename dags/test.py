from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime


# 실패 알림 함수
def task_fail_slack_alert(context):
    return SlackWebhookOperator(
        task_id=f"notify_failure_{context['task_instance'].task_id}",
        slack_webhook_conn_id="slack",
        message=f"""
:red_circle: *Task Failed!*
*Task*: {context['task_instance'].task_id}
*DAG*: {context['dag'].dag_id}
*Execution Time*: {context['execution_date']}
""",
        username="airflow"
    ).execute(context=context)

# 일부러 실패하게 만드는 함수
def fail_function():
    raise Exception("💥 테스트 실패입니다!")

# DAG 정의
with DAG(
    dag_id="slack_failure_test",
    start_date=datetime(2025, 5, 25),
    catchup=False,
) as dag:

    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=fail_function,
        on_failure_callback=task_fail_slack_alert  # 실패 시 알림
    )

fail_task