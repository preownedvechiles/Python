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
SELECT teamsActivity,localizations,modifiedAt,description,objectType,createdAt,deletedAt,statusId,createdBy,tenantId,name,presentToAgent,modifiedBy,status,id FROM `thrio-prod-sti.sti.status` 
"""

try:
    # Connect to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server successfully!")

    truncate_query = "delete from edw.import.Status where tenantId='sti'"
    cursor.execute(truncate_query)
    print("Deleted STI Tenant data successfully!")
                
    # Execute the BigQuery query
    query_job = client.query(query)  
    results = query_job.result()  

    # Direct Insert query
    insert_query = """
    INSERT INTO import.Status (       teamsActivity,localizations,modifiedAt,description,objectType,createdAt,deletedAt,statusId,createdBy,tenantId,name,presentToAgent,modifiedBy,status,id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Insert results into SQL Server
    for row in results:
        data_to_insert = (
            row.teamsActivity,
            row.localizations,
            row.modifiedAt,
            row.description,
            row.objectType,
            row.createdAt,
            row.deletedAt,
            row.statusId,            
            row.createdBy,
            row.tenantId,
            row.name,
            row.presentToAgent,            
            row.modifiedBy,
            row.status,
            row.id
        )
        cursor.execute(insert_query, data_to_insert)

    # Commit transaction
    conn.commit()
    print("Data successfully inserted into SQL Server!")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close connections
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Database connection closed.")
