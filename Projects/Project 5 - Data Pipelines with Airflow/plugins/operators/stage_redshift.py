from datetime import datetime
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    Load data from S3 buckets to stage tables.
    
    Keyword arguments:
    * redshift_conn_id  -- Redshift connection ID
    * aws_credentials   -- AWS credentials
    * redshift_table    -- staging table name
    * s3_bucket         -- s3 bucket name
    * s3_key            -- s3 key
    * json_path         -- JSON path
    * file_type         -- loading file type (CSV or JSON)
    * ignore_headers    -- ignore headers in CSV (0 or 1)
    * delimiter         -- CSV field delimiter
    '''
    ui_color = '#358140'

    copy_json_statement = '''
    COPY {redshift_table} 
    FROM '{s3_path}'
    ACCESS_KEY_ID '{aws_access_key}'
    SECRET_ACCESS_KEY '{aws_secret}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    STATUPDATE OFF
    FORMAT AS JSON '{json_path}'
    '''

    copy_csv_statement = '''
        COPY {redshift_table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{aws_access_key}'
        SECRET_ACCESS_KEY '{aws_secret}'
        IGNOREHEADER {ignore_headers}
        DELIMITER '{delimiter}'
    '''

    @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                aws_credentials='',
                redshift_table='',
                s3_bucket='',
                s3_key='',
                json_path='',
                file_type='',
                ignore_headers=1,
                delimiter=',',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.redshift_table = redshift_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.file_type = file_type
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter


    def execute(self, context):
        self.log.info('--------------------------------------------------------------------')
        self.log.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' Starting loading stage tables')
        self.log.info('Trying to collect AWS Credentials')
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        self.log.info('Succesfully collected AWS Credentials')
        self.log.info('Trying to connect to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, key)

        if self.file_type == "json":
            copy_json_sql = StageToRedshiftOperator.copy_json_statement.format(
                self.redshift_table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
            redshift.run(copy_json_sql)

        if self.file_type == "csv":
            copy_csv_sql = StageToRedshiftOperator.copy_csv_to_sql.format(
                self.redshift_table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
            redshift.run(copy_csv_sql)
        self.log.info(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' Loading stage tables finished succesfully')
        self.log.info('--------------------------------------------------------------------')
