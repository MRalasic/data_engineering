from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
    Load data from staging tables to dimension table.
    
    Keyword arguments:
    * redshift_conn_id  -- Redshift connection ID
    * redshift_table    -- Target table name
    * sql_script        -- SQL Query used
    * insert_mode       -- How to insert the data to the target table (overwrite = overwrite the existing data; otherwise data will be appended)
    '''

    ui_color = '#80BD9E'

    truncate_sql_statement = '''
        TRUNCATE TABLE {redshift_table};
        COMMIT;
    '''
    
    insert_sql_statement = '''
        INSERT INTO {redshift_table}
        {sql_script};
        COMMIT
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 redshift_table='',
                 sql_script='',
                 insert_mode='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_table = redshift_table
        self.sql_script = sql_script
        self.insert_mode = insert_mode
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('--------------------------------------------------------------------')
        self.log.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' Starting loading dimension tables')
        self.log.info('Trying to connect to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.insert_mode == 'overwrite':
            self.log.info('Overwriting the data in the target table: {}'.format(self.redshift_table))
            self.log.info('Truncating dimension table: {}'.format(self.redshift_table))
            truncate_sql = LoadDimensionOperator.truncate_sql_statement.format(
                redshift_table=self.redshift_table
            )
            redshift.run(truncate_sql)

        self.log.info('Loading dimension table: {}' .format(self.redshift_table))
        insert_sql = LoadDimensionOperator.insert_sql_statement.format(
            table=self.table,
            sql_script=self.sql_script
        )
        redshift.run(insert_sql)

        self.log.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' Loading fact tables finished succesfully')
        self.log.info('--------------------------------------------------------------------')
