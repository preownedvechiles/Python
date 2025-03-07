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

SELECT distinct id,createdAt,terminatedAt,duration,queueEnterReason,workitemId,queueId,queueName,queueExitResult,userId,'sti' TenantId 

	FROM `thrio-prod-sti.sti.DATA_WORKITEMS_QUEUES` 

	WHERE DATE(_PARTITIONTIME) >= (SELECT max(DATE(_PARTITIONTIME))-1 FROM `thrio-prod-sti.sti.DATA_WORKITEMS_QUEUES`)

 

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

    MERGE INTO import.DataWorkitemsQueues AS target

    USING (SELECT ? AS id, ? AS createdAt, ? AS terminatedAt, ? AS duration,

                  ? AS queueEnterReason, ? AS workitemId, ? AS queueId, ? AS queueName, ? AS queueExitResult, 
                  
                  ? AS userId, ? AS TenantId)

    AS source

    ON target.id = source.id

    WHEN MATCHED THEN

        UPDATE SET duration = source.duration,

                   queueEnterReason = source.queueEnterReason, 
                   
                   queueExitResult = source.queueExitResult

    WHEN NOT MATCHED THEN

        INSERT (id, createdAt, terminatedAt, duration, queueEnterReason, workitemId, queueId, queueName, queueExitResult, userId, TenantId)

        VALUES (source.id, source.createdAt, source.terminatedAt, source.duration, source.queueEnterReason, 

		source.workitemId, source.queueId, source.queueName, 

		source.queueExitResult, source.userId, source.TenantId);

    """

 

    # Insert results into SQL Server

    for row in results:

        data_to_insert = (

		row.id, row.createdAt, row.terminatedAt, row.duration, row.queueEnterReason, 

		row.workitemId, row.queueId, row.queueName, 

		row.queueExitResult, row.userId, row.TenantId

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