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

SELECT  distinct id,createdAt,terminatedAt,userId,userName,statusId,status,statusName,campaignId,campaignName,duration,'sti' TenantId 
	
	FROM `thrio-prod-sti.sti.DATA_USERS_STATUS` 

	WHERE DATE(_PARTITIONTIME) >= (SELECT max(DATE(_PARTITIONTIME))-1 FROM `thrio-prod-sti.sti.DATA_USERS_STATUS`)


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

    MERGE INTO import.DataUsersStatus AS target

    USING (SELECT ? AS id, ? AS createdAt, ? AS terminatedAt, ? AS userId,

                  ? AS userName, ? AS statusId, ? AS status, ? AS statusName, ? AS campaignId,

                  ? AS campaignName, ? AS duration, ? AS TenantId)

    AS source

    ON target.id = source.id

    WHEN MATCHED THEN

        UPDATE SET createdAt = source.createdAt, terminatedAt = source.terminatedAt, UserId = source.UserId,

                   UserName = source.UserName, statusId = source.statusId, status = source.status,

                   statusName = source.statusName, campaignId = source.campaignId,

                   campaignName = source.campaignName, Duration = source.Duration, TenantId = source.TenantId

    WHEN NOT MATCHED THEN

        INSERT (id, createdAt, terminatedAt, userId, userName, statusId, status, statusName, campaignId, campaignName, duration, TenantId)

        VALUES (source.id, source.createdAt, source.terminatedAt, source.userId, source.userName, 
        
        	source.statusId, source.status, source.statusName, source.campaignId, source.campaignName, 
		
		source.duration, source.TenantId);

    """

 

    # Insert results into SQL Server

    for row in results:

        data_to_insert = (

            row.id, row.createdAt, row.terminatedAt, row.userId, row.userName, 
	            
	    row.statusId, row.status, row.statusName, row.campaignId, row.campaignName, 
	    		
		row.duration, row.TenantId

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
