import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop the tables if they (some of them) already exist.
    
    Args:
        cur: A cursor instance within the 'conn' connection to the database
        conn: A connection instance of psycopg2 to the Redshift clusters
        
    Return:
        Nothing. Just updating the database by running some queries.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables by parsing the queries
    
    Args:
        cur: A cursor instance within the 'conn' connection to the database
        conn: A connection instance of psycopg2 to the Redshift clusters
        
    Return:
        Nothing. Just updating the database by running some queries.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """The main script
    Load the config file, connect to the Redshift cluster.
    Run the above two functions.
    
    Args:
        Nothing.
        
    Return:
        Nothing. Just run the previous two functions and update the database.
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