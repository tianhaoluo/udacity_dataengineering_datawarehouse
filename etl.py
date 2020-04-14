import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copy data from S3 to the staging tables.
    
    Args:
        cur: A cursor instance within the 'conn' connection to the database
        conn: A connection instance of psycopg2 to the Redshift clusters
        
    Return:
        Nothing. Just updating the database by running some queries.
    """
    for query in copy_table_queries:
        cur.execute(query)
        print("Successfully run query:")
        print(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data into the final tables by reorganizing data from staging tables.
    
    Args:
        cur: A cursor instance within the 'conn' connection to the database
        conn: A connection instance of psycopg2 to the Redshift clusters
        
    Return:
        Nothing. Just updating the database by running some queries.
    """
    for query in insert_table_queries:
        cur.execute(query)
        print("Successfully run query:")
        print(query)
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()