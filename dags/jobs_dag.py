from airflow import DAG, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timezone
import uuid

from postgres_operators import PostgreSqlCountRowsOperator


config = {
   'dag_id_1': {'schedule_interval': "", "start_date": datetime(2021, 10, 18), "tablename": "new_table_1", "queue": "jobs_queue"},  
   'dag_id_2': {'schedule_interval': "", "start_date": datetime(2018, 10, 18), "tablename": "new_table_2", "queue": "jobs_queue"},  
   'dag_id_3': {'schedule_interval': "", "start_date": datetime(2018, 10, 18), "tablename": "new_table_3", "queue": "jobs_queue"}
}


def query(ti):
    ti.xcom_push("message", "{{ run_id }} ended.")

def check_table_exists(tablename):
    sql_to_get_schema = "SELECT * FROM pg_tables;"
    hook = PostgresHook()
    # get schema name
    query = hook.get_records(sql=sql_to_get_schema)
    for result in query:
        if 'airflow' in result:
            schema = result[0]
            print(schema)
            break

        # check table exist
    sql_to_check_table_exist = "SELECT * FROM information_schema.tables " \
                               "WHERE table_schema = '{}'" \
                               "AND table_name = '{}';"
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, tablename))
    print(query)
    if query:
        return "insert_row"
    else:
        return "create_table"

# def query_postgres_table(tablename):
#     hook = PostgresHook()
#     sql = "SELECT COUNT(*) FROM %s" % (tablename)
#     query = hook.get_records(sql)
#     print(query)
#     return query

for dag_id, params in config.items():
    with DAG(dag_id=dag_id, default_args=params, ) as dag:
        process_start = DummyOperator(task_id="print_process_start")
        check_table = BranchPythonOperator(
            task_id="check_table_exists",
            python_callable=check_table_exists,
            op_kwargs={'tablename': params["tablename"]},
        )
        get_current_user = BashOperator(
            task_id="get_current_user", 
            bash_command="whoami",
        )
        create_table_sql = """CREATE TABLE %s (
        custom_id INTEGER NOT NULL,
        user_name VARCHAR (50) NOT NULL,
        timestamp TIMESTAMP NOT NULL);""" % (params["tablename"])
        create_table = PostgresOperator(task_id="create_table", sql=create_table_sql)

        insert_row_sql = "INSERT INTO {{ params.tablename }}" \
                         "   VALUES ( " \
                         "      {{ params.id }}, " \
                         "      '{{ti.xcom_pull(task_ids='get_current_user')}}', " \
                         "      TIMESTAMP '{{ params.timestamp }}');"
        insert_row = PostgresOperator(
            task_id="insert_row",
            sql=insert_row_sql,
            params={
                "timestamp": str(datetime.now(timezone.utc).replace(microsecond=0)),
                "tablename": params["tablename"],
                "id": uuid.uuid4().int % 123456789}, 
            trigger_rule=TriggerRule.NONE_FAILED
        )
        #query_table = PythonOperator(task_id="query_table", python_callable=query_postgres_table, op_kwargs={"tablename": params["tablename"]})
        query_table = PostgreSqlCountRowsOperator(
            task_id="query_table", 
            table=params["tablename"],
        )
        process_start >> get_current_user >> check_table >> [create_table, insert_row]
        create_table >> insert_row >> query_table
        globals()[dag_id] = dag