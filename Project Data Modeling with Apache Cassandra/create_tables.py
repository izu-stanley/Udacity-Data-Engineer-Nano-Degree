import cassandra
from cql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Conncect to Cassandra, drop existing db and create newones
    
    :params None
    :return Cassandra cluster and session
    """

    from cassandra.cluster import Cluster
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
    except Exception as e:
        print(e)
        
    try:
        session.execute("""
            DROP KEYSPACE IF EXISTS sparkifydb
        """)
    except Exception as e:
        print(e)
    try:
        session.execute("""
                CREATE KEYSPACE IF NOT EXISTS sparkifydb
                WITH REPLICATION =
                { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
    except Exception as e:
        print(e)

    try:
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    return cluster, session


def drop_tables(session):
    """
    Drop Existing tables in DB
    
    :params session: An apache cassandra session to execute queries
    :retrun None
    """
    for query in drop_table_queries:
        try:
            session.execute(query)
            print("Tables dropped!")
        except Exception as e:
            print("Error dropping table",e)

    

def create_tables(session):
    """
    Creates tables in DB
    
    :params session: A cassandra session
    :return None
    """
    for query in create_table_queries:
        try:
            session.execute(query)
            print("Tables created successfully")
        except Exception as e:
            print("Error creating table",e)
    

def main():
    """
    Connects to Cassandra 
    Creates DB 
    Drops existing tables 
    Creates Tables
    Closes connection
    
    :params None
    :return None
    """

    cluster, session = create_database()
    drop_tables(session)
    create_tables(session)
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
