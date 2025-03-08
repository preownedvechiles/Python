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
SELECT smsFromAddress,recordingAnalysisPercentage,answeringMachinePromptId,machineDetectionTimeout,maxDialRatio,abandonPercentage,humanFunction,noAnswerCallbackInMinutes,recordingAnalysisMinDuration,chatKeepAliveTimeoutForMobile,coolOffPeriodInSec,campaignId,dlpInfoTypes,thirdPartySkillFieldName,recordingAnalysisLanguages,humanDispositionId,defaultOutbound,smsFailedFucntionId,useForSMS,answeringMachineDispositionId,dialRatio,stateDIDId,calabrioPenaltyBox,answeringMachineAction,amdUnknownAsVoicemail,abandonDispositionId,busyCallbackInMinutes,useForProgressive,firstPartySkillFieldName,filterOnLeads,modifiedBy,initialStateId,chatKeepAliveTimeout,recordingAnalysisEndTime,localizations,emailFailedFucntionId,holdPrompt,abandonCallbackInMinutes,invalidDispositionId,emailaccountId,smsPerMinute,busyDispositionId,leadsDaysPartition,tenantId,emailTemplateId,machineDetectionSpeechEndThresholdInMillis,busyAction,predictiveQuaterback,numberOfAgentsToKeepForInbound,screenRecordingType,addresses,surveyId,maxAttemptsPerAddress,modifiedAt,calabrioContactTraces,groupId,thirdPartySkillCallbackTime,useForEmail,primaryPhoneField,objectType,campaignGoalsId,defaultExtension,createdAt,finalWorkitemStateId,noAnswerDispositionId,answeringMachineCallbackInMinutes,noAnswerTimeout,calabrioRecordings,abandonFunction,smsTemplateId,maxLeadsInMemory,filterId,recordingAnalysisMaxDuration,maxAttempts,deletedAt,finalUserStateId,humanAction,useForPredictive,abandonAction,name,useForOutbound,predictiveRestcallId,callerId,priorityCallbacks,fileServerId,recordingPercentage,redactionType,dailyMaxAttempts,minLeadsInMemory,description,noAnswerAction,states,emailPerMinute,holdMusicUrl,invalidCallbackInMinutes,enhancedAMD,leadOrderByField,faxDispositionId,emailFromAddress,preSurveyId,machineDetectionSilenceTimeout,fieldMappingsId,noAnswerFunction,machineDetectionSpeechThresholdInMillis,createdBy,applyRecordingConsent,emailSuccessFucntionId,busyFunction,smsSuccessFucntionId,recordingAnalysisStartTime,useForFax,disableRecordingOnTwoParty,id FROM `thrio-prod-sti.sti.campaign` 
"""

try:
    # Connect to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server successfully!")

    truncate_query = "delete from import.Campaign where tenantId='sti'"
    cursor.execute(truncate_query)
    print("Deleted STI Tenant data successfully!")
                
    # Execute the BigQuery query
    query_job = client.query(query)  
    results = query_job.result()  

    # Direct Insert query
    insert_query = """
    INSERT INTO import.Campaign (      smsFromAddress,recordingAnalysisPercentage,answeringMachinePromptId,machineDetectionTimeout,maxDialRatio,abandonPercentage,humanFunction,noAnswerCallbackInMinutes,recordingAnalysisMinDuration,chatKeepAliveTimeoutForMobile,coolOffPeriodInSec,campaignId,dlpInfoTypes,thirdPartySkillFieldName,recordingAnalysisLanguages,humanDispositionId,defaultOutbound,smsFailedFucntionId,useForSMS,answeringMachineDispositionId,dialRatio,stateDIDId,calabrioPenaltyBox,answeringMachineAction,amdUnknownAsVoicemail,abandonDispositionId,busyCallbackInMinutes,useForProgressive,firstPartySkillFieldName,filterOnLeads,modifiedBy,initialStateId,chatKeepAliveTimeout,recordingAnalysisEndTime,localizations,emailFailedFucntionId,holdPrompt,abandonCallbackInMinutes,invalidDispositionId,emailaccountId,smsPerMinute,busyDispositionId,leadsDaysPartition,tenantId,emailTemplateId,machineDetectionSpeechEndThresholdInMillis,busyAction,predictiveQuaterback,numberOfAgentsToKeepForInbound,screenRecordingType,addresses,surveyId,maxAttemptsPerAddress,modifiedAt,calabrioContactTraces,groupId,thirdPartySkillCallbackTime,useForEmail,primaryPhoneField,objectType,campaignGoalsId,defaultExtension,createdAt,finalWorkitemStateId,noAnswerDispositionId,answeringMachineCallbackInMinutes,noAnswerTimeout,calabrioRecordings,abandonFunction,smsTemplateId,maxLeadsInMemory,filterId,recordingAnalysisMaxDuration,maxAttempts,deletedAt,finalUserStateId,humanAction,useForPredictive,abandonAction,name,useForOutbound,predictiveRestcallId,callerId,priorityCallbacks,fileServerId,recordingPercentage,redactionType,dailyMaxAttempts,minLeadsInMemory,description,noAnswerAction,states,emailPerMinute,holdMusicUrl,invalidCallbackInMinutes,enhancedAMD,leadOrderByField,faxDispositionId,emailFromAddress,preSurveyId,machineDetectionSilenceTimeout,fieldMappingsId,noAnswerFunction,machineDetectionSpeechThresholdInMillis,createdBy,applyRecordingConsent,emailSuccessFucntionId,busyFunction,smsSuccessFucntionId,recordingAnalysisStartTime,useForFax,disableRecordingOnTwoParty,id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Insert results into SQL Server
    for row in results:
        data_to_insert = (
            row.smsFromAddress,
            row.recordingAnalysisPercentage,
            row.answeringMachinePromptId,
            row.machineDetectionTimeout,
            row.maxDialRatio,
            row.abandonPercentage,
            row.humanFunction,
            row.noAnswerCallbackInMinutes,
            row.recordingAnalysisMinDuration,
            row.chatKeepAliveTimeoutForMobile,
            row.coolOffPeriodInSec,
            row.campaignId,
            row.dlpInfoTypes,
            row.thirdPartySkillFieldName,
            row.recordingAnalysisLanguages,
            row.humanDispositionId,
            row.defaultOutbound,
            row.smsFailedFucntionId,
            row.useForSMS,
            row.answeringMachineDispositionId,
            row.dialRatio,
            row.stateDIDId,
            row.calabrioPenaltyBox,
            row.answeringMachineAction,
            row.amdUnknownAsVoicemail,
            row.abandonDispositionId,
            row.busyCallbackInMinutes,
            row.useForProgressive,
            row.firstPartySkillFieldName,
            row.filterOnLeads,
            row.modifiedBy,
            row.initialStateId,
            row.chatKeepAliveTimeout,
            row.recordingAnalysisEndTime,
            row.localizations,
            row.emailFailedFucntionId,
            row.holdPrompt,
            row.abandonCallbackInMinutes,
            row.invalidDispositionId,
            row.emailaccountId,
            row.smsPerMinute,
            row.busyDispositionId,
            row.leadsDaysPartition,
            row.tenantId,
            row.emailTemplateId,
            row.machineDetectionSpeechEndThresholdInMillis,
            row.busyAction,
            row.predictiveQuaterback,
            row.numberOfAgentsToKeepForInbound,
            row.screenRecordingType,
            row.addresses,
            row.surveyId,
            row.maxAttemptsPerAddress,
            row.modifiedAt,
            row.calabrioContactTraces,
            row.groupId,
            row.thirdPartySkillCallbackTime,
            row.useForEmail,
            row.primaryPhoneField,
            row.objectType,
            row.campaignGoalsId,
            row.defaultExtension,
            row.createdAt,
            row.finalWorkitemStateId,
            row.noAnswerDispositionId,
            row.answeringMachineCallbackInMinutes,
            row.noAnswerTimeout,
            row.calabrioRecordings,
            row.abandonFunction,
            row.smsTemplateId,
            row.maxLeadsInMemory,
            row.filterId,
            row.recordingAnalysisMaxDuration,
            row.maxAttempts,
            row.deletedAt,
            row.finalUserStateId,
            row.humanAction,
            row.useForPredictive,
            row.abandonAction,
            row.name,
            row.useForOutbound,
            row.predictiveRestcallId,
            row.callerId,
            row.priorityCallbacks,
            row.fileServerId,
            row.recordingPercentage,
            row.redactionType,
            row.dailyMaxAttempts,
            row.minLeadsInMemory,
            row.description,
            row.noAnswerAction,
            row.states,
            row.emailPerMinute,
            row.holdMusicUrl,
            row.invalidCallbackInMinutes,
            row.enhancedAMD,
            row.leadOrderByField,
            row.faxDispositionId,
            row.emailFromAddress,
            row.preSurveyId,
            row.machineDetectionSilenceTimeout,
            row.fieldMappingsId,
            row.noAnswerFunction,
            row.machineDetectionSpeechThresholdInMillis,
            row.createdBy,
            row.applyRecordingConsent,
            row.emailSuccessFucntionId,
            row.busyFunction,
            row.smsSuccessFucntionId,
            row.recordingAnalysisStartTime,
            row.useForFax,
            row.disableRecordingOnTwoParty,
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
