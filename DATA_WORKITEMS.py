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

SELECT distinct id,createdAt,terminatedAt,dispositionAt,fromAddress,toAddress,campaignId,campaignName,channelType,

	externalId,dispositionId,dispositionName,categoryId,categoryName,inReviewTime,ringTime,workflowTime,queueTime,

	talkTime,wrapupTime,workitemTime,agentMessageCount,consumerMessageCount,botMessageCount,

	terminationReason,contactId,listId,listName,leadId,listTableName,smsAgentMessageCount,

	smsConsumerMessageCount,smsBotMessageCount,holdTime,transferred,abandoned,outWaitTime,

	amdConnectTime,'sti' TenantId FROM `thrio-prod-sti.sti.DATA_WORKITEMS` WHERE DATE(_PARTITIONTIME) >= (

	SELECT max(DATE(_PARTITIONTIME))-1 FROM `thrio-prod-sti.sti.DATA_WORKITEMS`)

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

    MERGE INTO import.DataWorkitems AS target

    USING (SELECT 
		  ? AS 	id
		, ? AS 	createdAt
		, ? AS 	terminatedAt
		, ? AS 	dispositionAt
		, ? AS 	fromAddress
		, ? AS 	toAddress
		, ? AS 	campaignId
		, ? AS 	campaignName
		, ? AS 	channelType
		, ? AS 	externalId
		, ? AS 	dispositionId
		, ? AS 	dispositionName
		, ? AS 	categoryId
		, ? AS 	categoryName
		, ? AS 	inReviewTime
		, ? AS 	ringTime
		, ? AS 	workflowTime
		, ? AS 	queueTime
		, ? AS 	talkTime
		, ? AS 	wrapupTime
		, ? AS 	workitemTime
		, ? AS 	agentMessageCount
		, ? AS 	consumerMessageCount
		, ? AS 	botMessageCount
		, ? AS 	terminationReason
		, ? AS 	contactId
		, ? AS 	listId
		, ? AS 	listName
		, ? AS 	leadId
		, ? AS 	listTableName
		, ? AS 	smsAgentMessageCount
		, ? AS 	smsConsumerMessageCount
		, ? AS 	smsBotMessageCount
		, ? AS 	holdTime
		, ? AS 	transferred
		, ? AS 	abandoned
		, ? AS 	outWaitTime
		, ? AS 	amdConnectTime
		, ? AS 	TenantId
)

    AS source

    ON target.id = source.id AND target.createdAt = source.createdAt AND target.terminatedAt = source.terminatedAt

    WHEN MATCHED THEN

        UPDATE SET dispositionAt = source.dispositionAt,
			fromAddress = source.fromAddress,
			toAddress = source.toAddress,
			campaignId = source.campaignId,
			campaignName = source.campaignName,
			channelType = source.channelType,
			externalId = source.externalId,
			dispositionId = source.dispositionId,
			dispositionName = source.dispositionName,
			categoryId = source.categoryId,
			categoryName = source.categoryName,
			inReviewTime = source.inReviewTime,
			ringTime = source.ringTime,
			workflowTime = source.workflowTime,
			queueTime = source.queueTime,
			talkTime = source.talkTime,
			wrapupTime = source.wrapupTime,
			workitemTime = source.workitemTime,
			agentMessageCount = source.agentMessageCount,
			consumerMessageCount = source.consumerMessageCount,
			botMessageCount = source.botMessageCount,
			terminationReason = source.terminationReason,
			contactId = source.contactId,
			listId = source.listId,
			listName = source.listName,
			leadId = source.leadId,
			listTableName = source.listTableName,
			smsAgentMessageCount = source.smsAgentMessageCount,
			smsConsumerMessageCount = source.smsConsumerMessageCount,
			smsBotMessageCount = source.smsBotMessageCount,
			holdTime = source.holdTime,
			transferred = source.transferred,
			abandoned = source.abandoned,
			outWaitTime = source.outWaitTime,
			amdConnectTime = source.amdConnectTime,
			TenantId = source.TenantId


    WHEN NOT MATCHED THEN

        INSERT (id, createdAt, terminatedAt, dispositionAt, fromAddress, toAddress, campaignId, campaignName, channelType, 

		externalId, dispositionId, dispositionName, categoryId, categoryName, inReviewTime, ringTime, workflowTime, queueTime, 

		talkTime, wrapupTime, workitemTime, agentMessageCount, consumerMessageCount, botMessageCount, 

		terminationReason, contactId, listId, listName, leadId, listTableName, smsAgentMessageCount, 

		smsConsumerMessageCount, smsBotMessageCount, holdTime, transferred, abandoned, outWaitTime, 

		amdConnectTime, TenantId)

        VALUES (source.id, source.createdAt, source.terminatedAt, source.dispositionAt, source.fromAddress, source.toAddress, source.campaignId, source.campaignName, source.channelType, source.

		externalId, source.dispositionId, source.dispositionName, source.categoryId, source.categoryName, source.inReviewTime, source.ringTime, source.workflowTime, source.queueTime, source.

		talkTime, source.wrapupTime, source.workitemTime, source.agentMessageCount, source.consumerMessageCount, source.botMessageCount, source.

		terminationReason, source.contactId, source.listId, source.listName, source.leadId, source.listTableName, source.smsAgentMessageCount, source.

		smsConsumerMessageCount, source.smsBotMessageCount, source.holdTime, source.transferred, source.abandoned, source.outWaitTime, source.

		amdConnectTime, source.TenantId);

    """

 

    # Insert results into SQL Server

    for row in results:

        data_to_insert = (row.id, row.createdAt, row.terminatedAt, row.dispositionAt, row.fromAddress, row.toAddress, row.campaignId, row.campaignName, row.channelType, row.

		externalId, row.dispositionId, row.dispositionName, row.categoryId, row.categoryName, row.inReviewTime, row.ringTime, row.workflowTime, row.queueTime, row.

		talkTime, row.wrapupTime, row.workitemTime, row.agentMessageCount, row.consumerMessageCount, row.botMessageCount, row.

		terminationReason, row.contactId, row.listId, row.listName, row.leadId, row.listTableName, row.smsAgentMessageCount, row.

		smsConsumerMessageCount, row.smsBotMessageCount, row.holdTime, row.transferred, row.abandoned, row.outWaitTime, row.

		amdConnectTime, row.TenantId)

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
