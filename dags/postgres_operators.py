from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class PostgreSqlCountRowsOperator(BaseOperator):
    def __init__(self, table, column_name = "*", postgres_conn_id = None, **kwargs):
        super().__init__(**kwargs)
        self.table = table
        self.column_name = column_name
        self.postgres_conn_id = None
    
    def execute(self, context):
        if self.postgres_conn_id is not None:
            hook = PostgresHook(postgres_conn_id = self.postgres_conn_id)
        else:
            hook = PostgresHook()
        sql = "SELECT COUNT(%s) FROM %s" % (self.column_name, self.table)
        query = hook.get_records(sql)
        print(query)
        return query