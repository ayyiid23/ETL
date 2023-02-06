#from snowflake.connector.pandas_tools import write_pandas
#from snowflake.connector import pandas_tools
import snowflake.connector
import ETL_connection
import ETL_connection as conn
import ETL_connection as cur


ETL_connection.connect() 
ETL_connection.setup(database_to_use="BHATBHATENI", schema_to_use="DWH_STAGING")


#cur.execute("USE SCHEMA DWH_STAGING;")

#FOR COUNTRY
country_temp = """INSERT INTO DWH_TEMP.COUNTRY_TEMP (CNTRY_ID, CNTRY_DESC) 
SELECT ID,  COUNTRY_DESC FROM DWH_STAGING.COUNTR_STG;"""

ETL_connection.execute_command(command=country_temp)
print("Country Inserted to TEMP\n")

#FOR REGION
region_temp = """INSERT INTO DWH_TEMP.REGION_TEMP (RGN_ID, RGN_DESC) 
SELECT ID, REGION_DESC FROM DWH_STAGING.REGION_STG;"""

ETL_connection.execute_command(command=region_temp)
print("Region Inserted to TEMP\n")

#FOR STORE
store_temp = """INSERT INTO DWH_TEMP.store_TEMP (STORE_ID, STORE_DESC) 
SELECT ID,  STORE_DESC FROM DWH_STAGING.STORE_STG;"""

ETL_connection.execute_command(command=store_temp)
print("Store Inserted to TEMP\n")

# ALT METHOD:  ROW_NUMBER() OVER(ORDER BY ID)