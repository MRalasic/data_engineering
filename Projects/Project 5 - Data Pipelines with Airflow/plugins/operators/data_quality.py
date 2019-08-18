from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    data_quality_sql_statement = '''
        SELECT 
        COUNT(*)
        FROM {redshift_table}
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 redshift_table='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_table = redshift_table

    def execute(self, context):
        self.log.info('--------------------------------------------------------------------')
        self.log.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' Starting Data Quality check')
        self.log.info('Trying to connect to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        data_quality_sql=DataQualityOperator.data_quality_sql_statement.format(
            redshift_table=self.redshift_table
            )
        records = redshift_hook.get_records(data_quality_sql)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed: {} returned no results".format(self.redshift_table))
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. {} contained 0 rows".format(self.redshift_table))
        self.log.info("Data quality on table {0} check passed with {1} records".format(self.redshift_table, num_records))