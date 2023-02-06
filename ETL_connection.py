from snowflake.connector import pandas_tools
import snowflake.connector


conn=snowflake.connector.connect(
    user='diyaaryal',
    password='Manashwi@123',
    account='dt69880.central-india.azure',
    database='BHATBHATENI',
    warehouse='COMPUTE_WH'
)
cur= conn.cursor()

#try:
#    cur.execute("Select * from MY_DB.EMP")
#    one_row= cur.fetchall()
#    print(one_row)
#finally:
#    cur.close()
#conn.close()

#OR

def connect():
    conn=snowflake.connector.connect(
            user='diyaaryal',
            password='Manashwi@123',
            account='dt69880.central-india.azure',
            database='BHATBHATENI',
            warehouse='COMPUTE_WH'
            )
    cur= conn.cursor()
    print("Connected to Snowflake\n")

def close_connection():
    cur.close()
    conn.close()
    print("Snowflake Disconnected\n")


def setup(database_to_use,schema_to_use):
    conn.cursor().execute("USE WAREHOUSE COMPUTE_WH")
    conn.cursor().execute("USE DATABASE {}".format(database_to_use))
    conn.cursor().execute("USE SCHEMA {}".format(schema_to_use))

def execute_command(command):
    conn.cursor().execute(command)