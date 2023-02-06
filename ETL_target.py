import snowflake.connector
import ETL_connection
from ETL_connection import conn


ETL_connection.connect() 
ETL_connection.setup(database_to_use="BHATBHATENI", schema_to_use="DWH_TEMP")

def execute_command(command):
    conn.cursor().execute(command)

#FOR COUNTRY

# USED IDENTITY(1,1) FOR SURROGATE KEY 
#Type 1

country_target = """ MERGE INTO DWH_TARGET.COUNTRY_TARGET AS T
USING DWH_TEMP.COUNTRY_TEMP AS S
ON T.CNTRY_ID = S.CNTRY_ID

WHEN MATCHED THEN
UPDATE SET
 T.ACTIVEFLAG = 'N',
 T.UPDATE_TMS = CURRENT_TIMESTAMP()
 
WHEN NOT MATCHED THEN
INSERT (T.CNTRY_ID, T.CNTRY_DESC, T.ACTIVEFLAG, T.INSERT_TMS, T.UPDATE_TMS)
VALUES (S.CNTRY_ID, S.CNTRY_DESC, 'Y', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());"""

execute_command(command=country_target)
print("Country Loaded to Target\n")

#FOR REGION
# Type 2

region_target = """INSERT INTO DWH_TARGET.REGION_TARGET  
(RGN_ID, RGN_DESC, ACTIVEFLAG, INSERT_TMS, UPDATE_TMS)
SELECT S.RGN_ID,  S.RGN_DESC, 'Y', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
FROM  DWH_TEMP.REGION_TEMP S
WHERE S.RGN_ID NOT IN (SELECT RGN_ID FROM DWH_TARGET.REGION_TARGET);"""

reg= """ UPDATE DWH_TARGET.REGION_TARGET tgt
                SET
                tgt.RGN_ID = tmp.RGN_ID,
                tgt.RGN_DESC = tmp.RGN_DESC,
                tgt.UPDATE_TMS = CURRENT_TIMESTAMP()
                FROM DWH_TEMP.REGION_TEMP tmp
                WHERE tgt.RGN_ID =tmp.RGN_ID;"""

execute_command(command=region_target)
execute_command(command=reg)
print("Region Loaded to Target\n")

#FOR STORE

third="""update DWH_TARGET.STORE_TARGET as T
            SET T.ACTIVEFLAG='Y', T.UPDATE_TMS = current_timestamp()
            where T.STORE_KY NOT IN (SELECT STORE_KY from DWH_TEMP.STORE_TEMP)"""

store_target = """MERGE INTO DWH_TARGET.STORE_TARGET AS T
	USING DWH_TEMP.STORE_TEMP AS S
    ON T.STORE_ID = S.STORE_ID
    
	WHEN MATCHED AND
	T.STORE_ID <> S.STORE_ID or
    T.STORE_DESC <> S.STORE_DESC or
    T.ACTIVEFLAG = 'Y'

	THEN UPDATE SET
	T.STORE_ID = S.STORE_ID,
	T.STORE_DESC = S.STORE_DESC,
	T.ACTIVEFLAG = 'N',
	T.UPDATE_TMS = CURRENT_TIMESTAMP
	
	WHEN NOT MATCHED THEN
    INSERT (T.STORE_ID, T.STORE_DESC, T.ACTIVEFLAG, T.INSERT_TMS, T.UPDATE_TMS)
    VALUES (S.STORE_ID, S.STORE_DESC, 'Y', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());"""

execute_command(command=third)
execute_command(command=store_target)
print("Store Loaded to Target\n")