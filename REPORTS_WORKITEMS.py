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
password = '!'  # Consider using environment variables for security

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
SELECT DISTINCT Id, createdAt, terminatedAt, 
                JSON_VALUE(Object, '$.to') AS Object_ToNumber,
                JSON_VALUE(Object, '$.data.workitemId') AS Object_DataWorkitemId 
FROM `thrio-prod-sti.sti.REPORTS_WORKITEMS`
WHERE DATE(_PARTITIONTIME) >= (SELECT MAX(DATE(_PARTITIONTIME)) - 1 
                                FROM `thrio-prod-sti.sti.REPORTS_WORKITEMS`)
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
    MERGE INTO import.DAP10012_ReportsWorkitems AS target
    USING (SELECT ? AS [Id], ? AS [createdAt], ? AS [terminatedAt], 
                  ? AS [Object_ToNumber], ? AS [Object_DataWorkitemId])
    AS source
    ON target.[Id] = source.[Id] 
       AND target.[createdAt] = source.[createdAt] 
       AND target.[terminatedAt] = source.[terminatedAt]
    WHEN MATCHED THEN
        UPDATE SET [Object_ToNumber] = source.[Object_ToNumber], 
                   [Object_DataWorkitemId] = source.[Object_DataWorkitemId]
    WHEN NOT MATCHED THEN
        INSERT ([Id], [createdAt], [terminatedAt], [Object_ToNumber], [Object_DataWorkitemId])
        VALUES (source.[Id], source.[createdAt], source.[terminatedAt],
                source.[Object_ToNumber], source.[Object_DataWorkitemId]);
    """

    # Insert results into SQL Server
    for row in results:
        data_to_insert = (
            row.Id, row.createdAt, row.terminatedAt,
            row.Object_ToNumber, row.Object_DataWorkitemId
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
