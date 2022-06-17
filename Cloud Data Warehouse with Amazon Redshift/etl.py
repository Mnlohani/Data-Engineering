import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
     Dump/Load data from S3 into staging tables on Redshift
    
    Parameters:
    -----------
    cur: cursor to execute queries
    conn: Redshift cluster connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
     Load data into your analytics tables on Redshift from stagging tables.
    
    Parameters:
    -----------
    cur: cursor to execute queries
    conn: Redshift cluster connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Load DWH Params from dwh.cfg. 
    
    - Establishes connection with the Redshift cluster database and gets
    cursor to it.  
    
    - Load data into staging tables in Redshit from s3.  
    
    - Load data into your analytics tables on Redshift from stagging tables
    
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()