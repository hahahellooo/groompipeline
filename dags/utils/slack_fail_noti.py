from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from zoneinfo import ZoneInfo 

def task_fail_slack_alert(context):
    execution_date = context['execution_date'].astimezone(ZoneInfo("Asia/Seoul"))
    formatted_time = execution_date.strftime("%Y-%m-%d %H:%M:%S")
    return SlackWebhookOperator(
        task_id=f"notify_failure_{context['task_instance'].task_id}",  # 동적으로 유일하게
        slack_webhook_conn_id="slack",
        message=f"""
            :red_circle: Task Failed!
            *Task*: {context['task_instance'].task_id}
            *DAG*: {context['dag'].dag_id}
            *Execution Time*: {formatted_time}
        """,
        username="airflow"
    ).execute(context=context)