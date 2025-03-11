import os

from google.cloud import bigquery

import pyodbc

from datetime import datetime

import logging

logging.basicConfig(level=logging.INFO)

currentdatetime = datetime.now()  # Dynamically fetch the current timestamp

print(currentdatetime) 

# Set Google credentials dynamically

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\gcb\thrio-prod-sti-058aff318be0.json"

 

# Initialize BigQuery client

client = bigquery.Client()

 

# Define SQL Server connection parameters

server = 'USPA01BISQL01'

database = 'EDW'

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

SELECT  distinct id, createdAt, surveyId, workitemId, fieldId, response, 'sti' as TenantId 

# , currentdatetime as CreatedTS, currentdatetime as LastModifiedTs

	FROM `thrio-prod-sti.sti.SURVEY_RESULTS`  
	
	WHERE DATE(_PARTITIONTIME) >= (SELECT max(DATE(_PARTITIONTIME))-1 FROM `thrio-prod-sti.sti.SURVEY_RESULTS`)

 

"""
 

try:

    # Connect to SQL Server

    conn = pyodbc.connect(conn_str)

    cursor = conn.cursor()

    logging.info("Connected to SQL Server successfully!")

 

    # Execute the BigQuery query

    query_job = client.query(query) 

    results = query_job.result() 

 

    # Insert or update query using MERGE

    merge_query = """

    MERGE INTO import.DAP10000_SurveyResults AS target

    USING (SELECT ? AS id, ? AS CreatedAt, ? AS surveyId, ? AS workitemId, ? AS fieldId, ? AS Response, ? AS TenantId, ? AS CreatedTS, ? AS LastModifiedTs)

    AS source

    ON target.id = source.id AND target.createdAt = source.createdAt AND target.TenantId = source.TenantId

    WHEN MATCHED THEN

        UPDATE SET surveyId = source.surveyId, workitemId = source.workitemId,

                   fieldId = source.fieldId, Response = source.Response,

                   LastModifiedTs = source.LastModifiedTs

    WHEN NOT MATCHED THEN

        INSERT (id, CreatedAt, surveyId, workitemId, fieldId, Response, TenantId,

                CreatedTS, LastModifiedTs)

        VALUES (source.id, source.CreatedAt, source.surveyId, source.workitemId, source.fieldId, source.Response, source.TenantId,

                source.CreatedTS, source.LastModifiedTs);

    """

 

    # Insert results into SQL Server

    for row in results:

        data_to_insert = (row.id, row.createdAt, row.surveyId, row.workitemId, row.fieldId, row.response, row.TenantId,

                currentdatetime, currentdatetime)

        cursor.execute(merge_query, data_to_insert)

 

    # Commit transaction

    conn.commit()

    logging.info("Data successfully inserted/updated in SQL Server!")


# except Exception as e:

except Exception as e:
    logging.error(f"Error during execution: {e}", exc_info=True)

 #   print(f"Error: {e}")

 

finally:

    # Close connections

    if cursor:

        cursor.close()

    if conn:

        conn.close()

    logging.info("Database connection closed.")
    
    print(currentdatetime)
