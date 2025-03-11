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
SELECT  distinct  id,timestamp,userId,callId,type,filename,duration,`from`,`to`,deletedAt,createdAt,events,'sti' TenantId FROM `thrio-prod-sti.sti.CALL_RECORDING` 
WHERE DATE(_PARTITIONTIME) > (SELECT max(DATE(_PARTITIONTIME))-1 MaxDate FROM `thrio-prod-sti.sti.CALL_RECORDING`)
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
    MERGE INTO import.CallRecording AS target
    USING (SELECT ? AS id, ? AS timestamp,? AS userId, ? AS callId,? AS type, ? AS filename, ? AS duration, ? AS from,? AS to, ? AS deletedAt, ? AS createdAt, ? AS events,? AS TenantId
    )     
    AS source
    ON target.id = source.id 
    WHEN MATCHED THEN
        UPDATE SET   timestamp = source.timestamp, userId = source.userId, callId= source.callId , 
                   type = source.type, filename = source.filename, duration = source.duration,
                   from = source.from,to = source.to, deletedAt = source.deletedAt, createdAt = source.createdAt,events = source.events,TenantId = source.TenantId
    WHEN NOT MATCHED THEN
        INSERT (id,timestamp,userId,callId,type,filename,duration,from,to,deletedAt,createdAt,events, TenantId )
        VALUES (source.id, source.timestamp, source.userId, 
                source.callId,source.type,source.filename, source.duration, getattr(source, 'from'), source.to, source.deletedAt,source.createdAt,source.events,source.TenantId);
    """

    # Insert results into SQL Server
    for row in results:
        data_to_insert = (
            row.id, row.timestamp, row.userId, 
            row.callId,row.type,row.filename, row.duration, getattr(row, 'from'), row.to, row.deletedAt,row.createdAt,row.events,row.TenantId
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
