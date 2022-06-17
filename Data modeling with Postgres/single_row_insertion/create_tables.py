import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """ 
    Create and connects to sparkify database
    
    returns:
    --------
    conn: connection object to the database
    cur:  cursor object to execute sql queries
    """ 
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur =  conn.cursor()
    
    # connect to sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE database sparkifydb WITH ENCODING 'UTF8' TEMPLATE template0")
    
    # close connection to default database
    conn.close()
    
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: can not create db connection")
        print(e)
    cur = conn.cursor()
    
    return conn, cur
    


def drop_tables(cur, conn):
    """
    Drops each table using the SQL queries in the `drop_table_queries` list.
    
    Parameters:
    -----------
    cur: cursor to eecute queries
    conn: db connection
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error Droping Table")
            print(e)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    
    Parameters:
    -----------
    cur: cursor to eecute queries
    conn: db connection
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error creating Table")
            print(e)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    conn, cur = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()