from ETL_connection import cur
import ETL_connection as con
import os

# Function to change into a .csv file
def extract_to_csv_file(path, q1):
    tableList = ["COUNTRY", "REGION", "STORE"]
    for t in tableList:
        #print(q1[t])
        cur.execute(q1[t])
        result = cur.fetch_pandas_all() #pull the records from Snowflake
        result.to_csv(path + t + ".csv", index=False)
    print('\nExecuted Queries to csv files')



def main():

    con.connect()

    path= os.getcwd()+'/csv_files/'
    print('Path is: \n',path)
    
    # Q1) Export Data from Location Hierarchy table in file format

    q1 = {
        "COUNTRY":" SELECT * FROM COUNTRY;",
        "REGION":" SELECT * FROM REGION;",
        "STORE":" SELECT * FROM STORE;",
    }

    extract_to_csv_file(path, q1)
    con.close_connection()
    #load_to_staging(conn, path)

main()