from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timezone, timedelta

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