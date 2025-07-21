from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def slack_failure_alert(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url

    message = (
        f":red_circle: Task *{task_id}* in DAG *{dag_id}* failed!\n"
        f"Execution date: {execution_date}\n"
        f"Logs: {log_url}"
    )

    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_connection")
    slack_hook.send(text=message)
