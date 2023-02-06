from snowflake.connector.pandas_tools import write_pandas
from ETL_connection import cur
from ETL_connection import conn
import pandas as pd
import csv
import os


#### Function to load into staging table ##################################################
def load_to_staging(conn,path):
    cur.execute("USE SCHEMA DWH_STAGING;")

    #print('\nExecuting Public')
    # USE DICTIONARY, KEY VALUE PAIR
    fileList = os.listdir(path)
    tableList = ["COUNTR_STG",
                 "REGION_STG", "STORE_STG"]
    #tableList=["STORE_STG"]
    i = 0
    #print(tableList)
    for table in tableList:
        print(fileList[i])
        cur.execute("truncate table {}".format(table))
        df = pd.read_csv(path + fileList[i], sep=",")
        print(df)
        write_pandas(conn, df, table_name=table)
        print(f'\nLoaded {table} to staging table')
        i+=1
        print (i)
    
def main():

    path= os.getcwd()+'/csv_files/'
    print('Path is: \n',path)

    load_to_staging(conn, path)

try:
    main()
finally:
    cur.close()
conn.close()