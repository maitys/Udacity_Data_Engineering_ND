import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
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