from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.variable import Variable

from datetime import datetime
import os

from sensors.smart_file_sensor import SmartFileSensor


default_args = {
    "schedule_interval": "",
    "start_date": datetime(2021, 10, 24)
}
dag_to_monitor = "dag_id_1"
path_to_run = Variable.get("path_to_run")



def print_result(ti):
    result = ti.xcom_pull("message")
    print(result)
    print(ti)

    
with DAG('trigger_dag', default_args=default_args) as dag:
    file_sensor = SmartFileSensor(
        task_id = 'sensor_wait_run_file',
        filepath=path_to_run,
        fs_conn_id='fs_default'
    )

    trigger_dag = TriggerDagRunOperator(task_id="trigger_dag", trigger_dag_id=dag_to_monitor, execution_date=" {{execution_date}} ")
    post_message_to_slack = SlackWebhookOperator(
        task_id="post_message_to_slack",
        http_conn_id="slack_connection",
        webhook_token=os.getenv("slack_webhook_url"),
        message="all_done",
        channel="#general",
    )

    with TaskGroup("Process_result") as tg1:
        check_if_dag_is_completed = ExternalTaskSensor(
            task_id="check_if_dag_is_completed",
            external_dag_id=dag_to_monitor,
            external_task_id="query_table",
            dag=dag,
        )

        print_results = PythonOperator(
            task_id = "print_results",
            python_callable=print_result,
            dag=dag,
        )

        remove_run = BashOperator(
            task_id="remove_run", 
            bash_command="rm {{ var.value.path_to_run }}",
            dag=dag,
        )

        finish = BashOperator(
            task_id="create_finished_timestamp",
            bash_command="touch %s/finished_{{ ts_nodash }}" % (path_to_run[:-3])
        )
        check_if_dag_is_completed >> print_results >> remove_run >> finish

    file_sensor >> trigger_dag >> tg1 >> post_message_to_slack