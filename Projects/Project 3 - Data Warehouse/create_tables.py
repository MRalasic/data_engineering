import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from datetime import datetime


def drop_tables(cur, conn):
    """
    Drop database tables
    """

    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Succesfully dropped a table: ' +query.split()[4])
        except psycopg2.Error as e:
            print('Error in dropping a table: ' +query.split()[4])
            print(e)



def create_tables(cur, conn):
    """
    Create database tables
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Succesfully created a table: ' +query.split()[5])
        except psycopg2.Error as e:
            print('Error in creating a table: ' +query.split()[5])
            print(e)


def main():
    print('--------------------------------------------------------------------')
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' Table creation process started')

    config = configparser.ConfigParser()
    print('Reading config')
    config.read('dwh.cfg')

    print('Trying to connect to the Redshift')    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Succesfully connected to the Redshift')
    cur = conn.cursor()

    print('Dropping existing tables')
    drop_tables(cur, conn)
    print('Creating tables')
    create_tables(cur, conn)

    conn.close()
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' Table creation process finished')
    print('--------------------------------------------------------------------')


if __name__ == "__main__":
    main()