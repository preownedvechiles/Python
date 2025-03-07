import os
from google.cloud import bigquery
import pyodbc

# Set Google credentials dynamically
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\gcp\thrio-prod-sti-058aff318be0.json"

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
SELECT  distinct workitemId ,filename ,	duration,id ,type ,createdAt,'sti' as TenantId
FROM `thrio-prod-sti.sti.SCREEN_RECORDING`
WHERE DATE(_PARTITIONTIME) >= (SELECT max(DATE(_PARTITIONTIME))-1 MaxDate FROM `thrio-prod-sti.sti.SCREEN_RECORDING` )
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
    MERGE INTO import.ScreenRecording AS target
    USING (SELECT ? AS workitemId,? AS filename,? AS duration,? AS id,? AS type,? AS createdAt, 
    ? AS TenantId, ) 
    AS source
    ON target.workitemId = source.workitemId and target.filename = source.filename and target.id = source.id and target.type = source.type and target.createdAt = source.createdAt and   target.TenantId = source.TenantId
    WHEN MATCHED THEN
        UPDATE SET   duration = source.duration
        
    WHEN NOT MATCHED THEN
        INSERT (workitemId ,filename ,duration,id ,type ,createdAt,TenantId)
        VALUES (source.workitemId,source.filename,source.duration,source.id,source.type, source.createdAt, source.TenantId);
    """

    # Insert results into SQL Server
    for row in results:
        data_to_insert = (
            row.workitemId,row.filename,row.duration,row.id,row.type,row.createdAt, row.TenantId
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
