import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Function takes curson and connection to the DB
        And loop over the copy_table_queries to Copy date to the staging tables 
        
        copy_table_queries are implemented in sql_queries.py
    """
    print("Start Adding to Staging Tables")
    COUNT = 1
    for query in copy_table_queries:
        print("Table Number : {}".format(COUNT))
        COUNT = COUNT+1
        cur.execute(query)
        conn.commit()
    print("Staging Tables Are Ready")


def insert_tables(cur, conn):
    """
        Function takes curson and connection to the DB
        And loop over the insert_table_queries to INSERT date to the FINAL tables 
        
        insert_table_queries are implemented in sql_queries.py
    """
    print("Start Adding to Final Tables")
    COUNT = 1
    for query in insert_table_queries:
        print("Table Number : {}".format(COUNT))
        COUNT = COUNT+1
        cur.execute(query)
        conn.commit()
    print("Final Tables Are Ready")


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