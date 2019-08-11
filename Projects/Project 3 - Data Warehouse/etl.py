import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from datetime import datetime


def load_staging_tables(cur, conn):
    """
    Load staging tables from S3 buckets
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Succesfully loaded a table: ' +query.split()[1])
        except psycopg2.Error as e:
            print('Error in loading a table: ' +query.split()[1])
            print(e)

def insert_tables(cur, conn):
    """
    Inserting the data to the dimension and fact tables from the staging tables
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Succesfully inserted data: ' +query.split()[2])
        except psycopg2.Error as e:
            print('Error in inserting data: ' +query.split()[2])
            print(e)


def main():
    print('--------------------------------------------------------------------')
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' ETL process started')
    config = configparser.ConfigParser()
    print('Reading config')
    config.read('dwh.cfg')

    print('Trying to connect to the Redshift') 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Succesfully connected to the Redshift')
    cur = conn.cursor()
    
    print('Loading staging tables')
    load_staging_tables(cur, conn)
    print('Inserting data into the dimension and fact tables')
    insert_tables(cur, conn)

    conn.close()
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' ETL process finished')
    print('--------------------------------------------------------------------')


if __name__ == "__main__":
    main()