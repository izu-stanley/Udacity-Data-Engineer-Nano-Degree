import configparser
import psycopg2
from sql_queries import copy_table_order, copy_table_queries, insert_table_order, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from logs into the staging tables for further processing
    
    :params cur: The cursor for executing queries in the database
    :params conn: The connection to the database
    
    :return None
    """
    count = 0
    for query in copy_table_queries:
        print("Loading data into {} ".format(copy_table_order[count]))
        cur.execute(query)
        conn.commit()
        count += 1
        print("Loaded Successfully")


def insert_tables(cur, conn):
    """
    Extract and Transform data from staging Tables
    Load the transformed data into another table for analysis
    
    :params cur: The cursor for executing queries in the database
    :params conn: The connection to the database
    
    :return None
    """
    count = 0
    for query in insert_table_queries:
        print("Loading data into {} ".format(insert_table_order[count]))
        cur.execute(query)
        conn.commit()
        count += 1
        print("Loaded Successfully")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()