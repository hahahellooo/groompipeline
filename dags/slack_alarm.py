from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

SLACK_DAG_CON_ID = "slack"


def send_message(slack_msg):
    return SlackWebhookOperator(
        task_id="send_message",
        http_conn_id=SLACK_DAG_CON_ID,
        message=slack_msg,
        username="airflow"
    )


def task_fail_slack_alert(context):
    slack_msg = f"""
                :red_circle: Task Failed.
                *Task*: {context.get("task_instance").task_id}
                *Dag*: {context.get("task_instance").dag_id}
                *Execution Time*: {context.get("execution_date")}
                """
    alert = send_message(slack_msg)

    return alert.execute(context=context)
