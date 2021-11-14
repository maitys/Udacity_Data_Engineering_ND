import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Summary:
        This function loads the staging files from S3 to staging tables in Redshift
    Args: 
        conn: for connection to database
        cur: for cursor object to execute queries
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Summary:
        This function loads data from staging tables in Redshift to final tables in Redshift
    Args: 
        conn: for connection to database
        cur: for cursor object to execute queries
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Summary:
        This function connects to the database and calls the load_staging_tables and insert_tables functions
    """
    config = configparser.ConfigParser()
    config.read('maitys_aws_dwh.cfg')
    
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_ENDPOINT           = config.get("DWH","DWH_ENDPOINT")
    
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("******Staging Tables Load Complete******")
    insert_tables(cur, conn)
    print("******Final Tables Load Complete******")

    conn.close()


if __name__ == "__main__":
    main()