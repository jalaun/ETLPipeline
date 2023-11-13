from sqlalchemy import create_engine
import pyodbc
import pandas as pd
from psycopg2 import connect


# sql db details
driver = "{ODBC Driver 17 for SQL Server}"
server = "xxxxx" #server name at top of Object Explorer in MSSQL
database = "AdventureWorks"


def extract():
    try:
        src_conn = pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes;')
        src_cursor = src_conn.cursor()
        print("Connected to SQL Server")

        # execute query
        src_cursor.execute("""SELECT t.name as table_name
                              FROM sys.tables t
                              WHERE t.name = 'myNewTable'""")
        src_tables = src_cursor.fetchall()

        if src_tables:
            print(f"Tables to be extracted: {[tbl[0] for tbl in src_tables]}")
        else:
            print("No tables found to extract.")

        for tbl in src_tables:
            print(f"Extracting data from {tbl[0]}")
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])

    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()


def load(df, tbl):
    try:
        rows_imported = 0
        # create a connection to postgres with sql alchemy create engine
        engine = create_engine('postgresql://etl:xxxx@xxxx:xxx/AdventureWorks')
        print(f"Starting data load for table {tbl}")

        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)

        print(f"Data imported successful for {tbl}, rows imported: {rows_imported}")

    except Exception as e:
        print(f"Data load error for {tbl}: " + str(e))


try:
    # call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
