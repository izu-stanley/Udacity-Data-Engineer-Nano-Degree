import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes queries in drop_table_queries which go on delete the reqired tables.
    
    :params cur: The cursor for executing queries in the database
    :params conn: The connection to the database
    
    :return None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes queries in create_table_queries which go on create the reqired tables.
    
    :params cur: The cursor for executing queries in the database
    :params conn: The connection to the database
    
    :return None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main Driver function
    Loads configuration paramerters from config file
    Creates Connection to DB
    Creartes Cursor object
    Closes connection to DB after queries have finished executing
    
    
    :params None
    
    :return None
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()