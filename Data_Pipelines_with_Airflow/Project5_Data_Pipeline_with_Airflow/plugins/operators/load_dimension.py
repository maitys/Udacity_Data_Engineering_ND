from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
    This Operator class loads data into a specified dimension table in Redshift.
    Depending on the specified mode, it either appends data to the table or first deletes existing data before inserting new data into the table using the provided SQL query.
    Input: redshift_conn_id, table, sql_query, mode of loading
    Output: logs the result of the data load after the data is loaded into the dimension tables
    '''
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql_query,
                 mode,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        self.log.info('Setup Connection to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == "delete-load":
            self.log.info(f"Deleting data from dimension table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")
            self.log.info(f"Loading data into dimension table {self.table}")
            redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
        elif self.mode == "append-only":
            self.log.info(f"Loading data into dimension table {self.table}")
            redshift.run(f"INSERT INTO {self.table} {self.sql_query}")
        else:
            raise ValueError(f"Invalid mode provided: {self.mode}. Expected 'delete-load' or 'append-only'.")
