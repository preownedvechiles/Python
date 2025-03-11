import os

import pyodbc



# Define SQL Server connection parameters
server = 'USPA01BISQL01'
database = 'DWH'
username = 'TL2020'
password = 'TEternal2021!'   # Consider using environment variables for security

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
SELECT dwCreatedAtDate, dwTerminatedAtDate, dwDispositionAtDate, workItemId, campaignId, campaignName,
       fromAddress, toAddress, channelTypeGroup, channelType, terminationReason, dispositionId, dispositionName,
       TenantId, queueId, queueName, row_num_queues, QUEUEMAPPINGID, PROGRAMID, PROGRAMIDTXT, Program, SubProgram,
       VoiceANSSLA_seconds, VoiceABNSLA_seconds, UserId, Username, UserFirstName, UserLastName, UserProfileId,
       UserProfileGroup, inReviewTime, ringTime, workflowTime, queueTime, talkTime, holdTime, wrapupTime, handleTime,
       agent_name, abandoned, abandonedBefore, abandonedAfter, answered, answeredBefore, answeredAfter, CallStartTime,
       CallEndTime, HoldCount, TransferCount, ConferenceCount, transferred, TransferDestination, ConsultTime,
       RingTimeInbound, LinkOutboundInbound, ATTUID, LocationName, HoldAbandoned, ExternalTransfer, UserTransfer,
       QueueTransfer, abandonterminated, busyterminated, errorterminated, noanswerterminated, terminated,
       terminatedonhold, userterminated, userterminatedphonehangup, userterminatedphonehanguponhold, workflowterminated,
       supervisorterminated, LanguageCode
FROM transformation.VW_DAP10000_FCDR_Thrio
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
    MERGE INTO Transformation.DAP10000_F_CDR_Thrio AS target
    USING (SELECT ? AS workItemId, ? AS dwCreatedAtDate, ? AS dwTerminatedAtDate, ? AS dwDispositionAtDate, 
                 ? AS campaignId, ? AS campaignName, ? AS fromAddress, ? AS toAddress, ? AS channelTypeGroup, 
                 ? AS channelType, ? AS terminationReason, ? AS dispositionId, ? AS dispositionName, ? AS TenantId, 
                 ? AS queueId, ? AS queueName, ? AS row_num_queues, ? AS QUEUEMAPPINGID, ? AS PROGRAMID, 
                 ? AS PROGRAMIDTXT, ? AS Program, ? AS SubProgram, ? AS VoiceANSSLA_seconds, ? AS VoiceABNSLA_seconds, 
                 ? AS UserId, ? AS Username, ? AS UserFirstName, ? AS UserLastName, ? AS UserProfileId, ? AS UserProfileGroup, 
                 ? AS inReviewTime, ? AS ringTime, ? AS workflowTime, ? AS queueTime, ? AS talkTime, ? AS holdTime, 
                 ? AS wrapupTime, ? AS handleTime, ? AS agent_name, ? AS abandoned, ? AS abandonedBefore, 
                 ? AS abandonedAfter, ? AS answered, ? AS answeredBefore, ? AS answeredAfter, ? AS CallStartTime, 
                 ? AS CallEndTime, ? AS ATTUID, ? AS HoldCount, ? AS TransferCount, ? AS ConferenceCount, 
                 ? AS transferred, ? AS TransferDestination, ? AS ConsultTime, ? AS RingTimeInbound, 
                 ? AS LinkOutboundInbound, ? AS LocationName, ? AS ExternalTransfer, ? AS UserTransfer, 
                 ? AS QueueTransfer, ? AS abandonterminated, ? AS busyterminated, ? AS errorterminated, 
                 ? AS noanswerterminated, ? AS terminated, ? AS terminatedonhold, ? AS userterminated, 
                 ? AS userterminatedphonehangup, ? AS userterminatedphonehanguponhold, ? AS workflowterminated, 
                 ? AS supervisorterminated, ? AS LanguageCode) AS source
    ON target.workItemId = source.workItemId
    WHEN MATCHED THEN
        UPDATE SET dwCreatedAtDate = source.dwCreatedAtDate, dwTerminatedAtDate = source.dwTerminatedAtDate, 
                  dwDispositionAtDate = source.dwDispositionAtDate, campaignId = source.campaignId, 
                  campaignName = source.campaignName, fromAddress = source.fromAddress, toAddress = source.toAddress, 
                  channelTypeGroup = source.channelTypeGroup, channelType = source.channelType, 
                  terminationReason = source.terminationReason, dispositionId = source.dispositionId, 
                  dispositionName = source.dispositionName, TenantId = source.TenantId, queueId = source.queueId, 
                  queueName = source.queueName, row_num_queues = source.row_num_queues, QUEUEMAPPINGID = source.QUEUEMAPPINGID, 
                  PROGRAMID = source.PROGRAMID, PROGRAMIDTXT = source.PROGRAMIDTXT, Program = source.Program, 
                  SubProgram = source.SubProgram, VoiceANSSLA_seconds = source.VoiceANSSLA_seconds, 
                  VoiceABNSLA_seconds = source.VoiceABNSLA_seconds, UserId = source.UserId, Username = source.Username, 
                  UserFirstName = source.UserFirstName, UserLastName = source.UserLastName, UserProfileId = source.UserProfileId, 
                  UserProfileGroup = source.UserProfileGroup, inReviewTime = source.inReviewTime, ringTime = source.ringTime, 
                  workflowTime = source.workflowTime, queueTime = source.queueTime, talkTime = source.talkTime, 
                  holdTime = source.holdTime, wrapupTime = source.wrapupTime, handleTime = source.handleTime, 
                  agent_name = source.agent_name, abandoned = source.abandoned, abandonedBefore = source.abandonedBefore, 
                  abandonedAfter = source.abandonedAfter, answered = source.answered, answeredBefore = source.answeredBefore, 
                  answeredAfter = source.answeredAfter, LastModifiedTs = GETDATE(), CallStartTime = source.CallStartTime, 
                  CallEndTime = source.CallEndTime, ATTUID = source.ATTUID, HoldCount = source.HoldCount, 
                  TransferCount = source.TransferCount, ConferenceCount = source.ConferenceCount, 
                  Transferred = source.transferred, TransferDestination = source.TransferDestination, 
                  ConsultTime = source.ConsultTime, RingTimeInbound = source.RingTimeInbound, 
                  LinkOutboundInbound = source.LinkOutboundInbound, LocationName = source.LocationName, 
                  ExternalTransfer = source.ExternalTransfer, UserTransfer = source.UserTransfer, 
                  QueueTransfer = source.QueueTransfer, abandonterminated = source.abandonterminated, 
                  busyterminated = source.busyterminated, errorterminated = source.errorterminated, 
                  noanswerterminated = source.noanswerterminated, terminated = source.terminated, 
                  terminatedonhold = source.terminatedonhold, userterminated = source.userterminated, 
                  userterminatedphonehangup = source.userterminatedphonehangup, 
                  userterminatedphonehanguponhold = source.userterminatedphonehanguponhold, 
                  workflowterminated = source.workflowterminated, supervisorterminated = source.supervisorterminated, 
                  LanguageCode = source.LanguageCode
    WHEN NOT MATCHED THEN
        INSERT (dwCreatedAtDate, dwTerminatedAtDate, dwDispositionAtDate, workItemId, campaignId, campaignName, 
                fromAddress, toAddress, channelTypeGroup, channelType, terminationReason, dispositionId, dispositionName, 
                TenantId, queueId, queueName, row_num_queues, QUEUEMAPPINGID, PROGRAMID, PROGRAMIDTXT, Program, SubProgram, 
                VoiceANSSLA_seconds, VoiceABNSLA_seconds, UserId, Username, UserFirstName, UserLastName, UserProfileId, 
                UserProfileGroup, inReviewTime, ringTime, workflowTime, queueTime, talkTime, holdTime, wrapupTime, handleTime, 
                agent_name, abandoned, abandonedBefore, abandonedAfter, answered, answeredBefore, answeredAfter, CreatedTs, 
                LastModifiedTs, CallStartTime, CallEndTime, ATTUID, HoldCount, TransferCount, ConferenceCount, Transferred, 
                TransferDestination, HoldAbandoned, ConsultTime, RingTimeInbound, LinkOutboundInbound, LocationName, 
                ExternalTransfer, UserTransfer, QueueTransfer, AbandonTerminated, BusyTerminated, ErrorTerminated, 
                NoAnswerTerminated, Terminated, TerminatedOnhold, UserTerminated, UserTerminatedPhoneHangup, 
                UserTerminatedPhoneHangupOnhold, WorkflowTerminated, SupervisorTerminated, LanguageCode)
        VALUES (source.dwCreatedAtDate, source.dwTerminatedAtDate, source.dwDispositionAtDate, source.workItemId, 
                source.campaignId, source.campaignName, source.fromAddress, source.toAddress, source.channelTypeGroup, 
                source.channelType, source.terminationReason, source.dispositionId, source.dispositionName, 
                source.TenantId, source.queueId, source.queueName, source.row_num_queues, source.QUEUEMAPPINGID, 
                source.PROGRAMID, source.PROGRAMIDTXT, source.Program, source.SubProgram, source.VoiceANSSLA_seconds, 
                source.VoiceABNSLA_seconds, source.UserId, source.Username, source.UserFirstName, source.UserLastName, 
                source.UserProfileId, source.UserProfileGroup, source.inReviewTime, source.ringTime, source.workflowTime, 
                source.queueTime, source.talkTime, source.holdTime, source.wrapupTime, source.handleTime, source.agent_name, 
                source.abandoned, source.abandonedBefore, source.abandonedAfter, source.answered, source.answeredBefore, 
                source.answeredAfter, GETDATE(), GETDATE(), source.CallStartTime, source.CallEndTime, source.ATTUID, 
                source.HoldCount, source.TransferCount, source.ConferenceCount, source.transferred, source.TransferDestination, 
                source.HoldAbandoned, source.ConsultTime, source.RingTimeInbound, source.LinkOutboundInbound, 
                source.LocationName, source.ExternalTransfer, source.UserTransfer, source.QueueTransfer, 
                source.abandonterminated, source.busyterminated, source.errorterminated, source.noanswerterminated, 
                source.terminated, source.terminatedonhold, source.userterminated, source.userterminatedphonehangup, 
                source.userterminatedphonehanguponhold, source.workflowterminated, source.supervisorterminated, 
                source.LanguageCode)
    """

    for row in results:
        # Define your data to insert
        data_to_insert = (
            row.dwCreatedAtDate, row.dwTerminatedAtDate, row.dwDispositionAtDate, row.workItemId, row.campaignId, 
            row.campaignName, row.fromAddress, row.toAddress, row.channelTypeGroup, row.channelType, row.terminationReason, 
            row.dispositionId, row.dispositionName, row.TenantId, row.queueId, row.queueName, row.row_num_queues, 
            row.QUEUEMAPPINGID, row.PROGRAMID, row.PROGRAMIDTXT, row.Program, row.SubProgram, row.VoiceANSSLA_seconds, 
            row.VoiceABNSLA_seconds, row.UserId, row.Username, row.UserFirstName, row.UserLastName, row.UserProfileId, 
            row.UserProfileGroup, row.inReviewTime, row.ringTime, row.workflowTime, row.queueTime, row.talkTime, row.holdTime, 
            row.wrapupTime, row.handleTime, row.agent_name, row.abandoned, row.abandonedBefore, row.abandonedAfter, row.answered, 
            row.answeredBefore, row.answeredAfter, row.CallStartTime, row.CallEndTime, row.HoldCount, row.TransferCount, 
            row.ConferenceCount, row.transferred, row.TransferDestination, row.ConsultTime, row.RingTimeInbound, 
            row.LinkOutboundInbound, row.ATTUID, row.LocationName, row.HoldAbandoned, row.ExternalTransfer, row.UserTransfer, 
            row.QueueTransfer, row.abandonterminated, row.busyterminated, row.errorterminated, row.noanswerterminated, 
            row.terminated, row.terminatedonhold, row.userterminated, row.userterminatedphonehangup, 
            row.userterminatedphonehanguponhold, row.workflowterminated, row.supervisorterminated, row.LanguageCode
        )
        cursor.execute(merge_query, data_to_insert)

    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully!")
    
except Exception as e:
    print(f"An error occurred: {e}")
