from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 append_data="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement= sql_statement
        self.append_data = append_data


    def execute(self, context):
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == 'True':
            self.log.info("Loading data into fact table in Redshift")
            table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement}
            """
        else:
            sql_statement = "DELETE FROM {}".format(self.table)
            redshift.run(sql_statement)
            table_insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement}
            """
            redshift_hook.run(table_insert_sql)