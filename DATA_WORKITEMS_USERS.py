import os
from google.cloud import bigquery
import pyodbc

# Set Google credentials dynamically
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\gcb\thrio-prod-sti-058aff318be0.json"

# Initialize BigQuery client
client = bigquery.Client()

# Define SQL Server connection parameters
server = 'USPA01BISQL01'
database = 'DWH'
username = 'TL2020'
password = 'TEternal2021!'  # Consider using environment variables for security

# Define the ODBC connection string
conn_str = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
"""

# Define the query to fetch data from BigQuery
query = """
SELECT  distinct id,createdAt,terminatedAt,duration,workitemId,userId,userName,'sti' TenantId,wrapupTime,holdTime,offerTime,transferOutType 
FROM `thrio-prod-sti.sti.DATA_WORKITEMS_USERS` 
WHERE DATE(_PARTITIONTIME) >= (SELECT max(DATE(_PARTITIONTIME))-1 MaxDate FROM `thrio-prod-sti.sti.DATA_WORKITEMS_USERS`)
"""

try:
    # Connect to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server successfully!")

    # Execute the BigQuery query
    query_job = client.query(query)  
    results = query_job.result()  

    # Insert or update query using MERGE
    merge_query = """
    MERGE INTO import.DataWorkitemsUsers AS target
    USING (SELECT ? AS id,  ? AS createdAt, ? AS terminatedAt, ? AS duration, ? AS workitemId,
                  ? AS userId, ? AS userName, ? AS TenantId, ? AS wrapupTime, ? AS holdTime,
                  ? AS offerTime,? AS transferOutType) 
    AS source
    ON target.id = source.id and target.createdAt = source.createdAt and target.terminatedAt = source.terminatedAt and target.workitemId = source.workitemId and target.TenantId = source.TenantId
    WHEN MATCHED THEN
        UPDATE SET   duration = source.duration, userId = source.userId, userName= source.userName , 
                   wrapupTime = source.wrapupTime, holdTime = source.holdTime, offerTime = source.offerTime, transferOutType = source.transferOutType
    WHEN NOT MATCHED THEN
        INSERT (id,createdAt,terminatedAt,duration,workitemId,userId,userName,TenantId,wrapupTime,holdTime,offerTime,transferOutType)
        VALUES (source.id, source.createdAt, source.terminatedAt, 
                source.duration,source.workitemId,source.userId, source.userName, source.TenantId, source.wrapupTime, source.holdTime, 
                source.offerTime,  source.transferOutType);
    """

    # Insert results into SQL Server
    for row in results:
        data_to_insert = (
            row.id,row.createdAt, row.terminatedAt, row.duration,row.workitemId,row.userId, row.userName, row.TenantId,row.wrapupTime, row.holdTime, row.offerTime,  row.transferOutType
                         )
        cursor.execute(merge_query, data_to_insert)

    # Commit transaction
    conn.commit()
    print("Data successfully inserted/updated in SQL Server!")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close connections
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Database connection closed.")
