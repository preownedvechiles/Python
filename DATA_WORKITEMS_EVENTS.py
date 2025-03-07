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
SELECT DISTINCT id, workitemId, createdAt, terminatedAt, duration, type, queues, 
                selectedQueue, priority, userId, dispositionId, complianceId, displayName, 
                'sti' AS TenantId
FROM `thrio-prod-sti.sti.DATA_WORKITEMS_EVENTS`
WHERE DATE(_PARTITIONTIME) >= (SELECT max(DATE(_PARTITIONTIME))-1  FROM `thrio-prod-sti.sti.DATA_WORKITEMS_EVENTS`)

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
    MERGE INTO import.DataWorkitemsEvents AS target
    USING (SELECT ? AS id, ? AS workitemId, ? AS createdAt, ? AS terminatedAt, ? AS duration, 
                  ? AS type, ? AS queues, ? AS selectedQueue, ? AS priority, ? AS userId, 
                  ? AS dispositionId, ? AS complianceId, ? AS displayName, ? AS TenantId) 
    AS source
    ON target.id = source.id and target.workitemId = source.workitemId and target.createdAt = source.createdAt and target.terminatedAt = source.terminatedAt
    WHEN MATCHED THEN
        UPDATE SET  duration = source.duration, 
                   type = source.type, queues = source.queues, selectedQueue = source.selectedQueue, 
                   priority = source.priority, userId = source.userId, 
                   dispositionId = source.dispositionId, complianceId = source.complianceId, 
                   displayName = source.displayName, TenantId = source.TenantId
    WHEN NOT MATCHED THEN
        INSERT (id, workitemId, createdAt, terminatedAt, duration, type, queues, 
                selectedQueue, priority, userId, dispositionId, complianceId, 
                displayName, TenantId)
        VALUES (source.id, source.workitemId, source.createdAt, source.terminatedAt, 
                source.duration, source.type, source.queues, source.selectedQueue, 
                source.priority, source.userId, source.dispositionId, source.complianceId, 
                source.displayName, source.TenantId);
    """

    # Insert results into SQL Server
    for row in results:
        data_to_insert = (
            row.id, row.workitemId, row.createdAt, row.terminatedAt, row.duration, row.type,
            row.queues, row.selectedQueue, row.priority, row.userId, row.dispositionId,
            row.complianceId, row.displayName, row.TenantId
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
