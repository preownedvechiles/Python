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
SELECT autoDispositionDelay, zipCode, modifiedAt, defaultAcdLoginStatus, recordingAnalysisPercentage, objectType, createdAt, hideInDirectory, password, emailAddress, userClientSettingsId, autoDispositionId, passwordModifiedAt, contentType, chatWelcome, dispositionDelay, callFailedNotification, dispositionId, allowAcdChanges, deviceInfo, deviceToken, firstName, deletedAt, edgeId, firebaseToken, phone, telephony, dob, referralLink, oAuthQueueAssignments, userProfileId, acdAutoAccept, lastName, extension, acdAutoLogin, role, gender, disableStartRecording, voicemailDropFilename, timezone, verifyConsentOnDial, welcomePromptId, pseudonym, directory, enabled, disableStopRecording, oAuthProfileAssignmentFromExternalRole, countryCode, scrubOnDial, modifiedBy, promoCode, dispositionNotification, credentialsNonExpired, languageId, emailNotificationDomain, disableDisposition, avatar, allowClientTracing, userId, allowServerTracing, voicemailGreetingFilename, createdBy, disableEmailNotification, dialConfirmation, tenantId, reminderTime, username, id, defaultExtensionCampaignId, hideInCompanyDirectory, hrid, location, name, ringPhoneOnOffer, showWebrtcNotification 
FROM `thrio-prod-sti.sti.user`
"""

try:
    # Connect to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server successfully!")

    truncate_query = "delete from edw.import.Users where tenantId='sti'"
    cursor.execute(truncate_query)
    print("Deleted STI Tenant data successfully!")
                
    # Execute the BigQuery query
    query_job = client.query(query)  
    results = query_job.result()  

    # Direct Insert query
    insert_query = """
    INSERT INTO import.users (
        autoDispositionDelay, zipCode, modifiedAt, defaultAcdLoginStatus, recordingAnalysisPercentage, objectType, createdAt, hideInDirectory, password, emailAddress, userClientSettingsId, autoDispositionId, passwordModifiedAt, contentType, chatWelcome, dispositionDelay, callFailedNotification, dispositionId, allowAcdChanges, deviceInfo, deviceToken, firstName, deletedAt, edgeId, firebaseToken, phone, telephony, dob, referralLink, oAuthQueueAssignments, userProfileId, acdAutoAccept, lastName, extension, acdAutoLogin, role, gender, disableStartRecording, voicemailDropFilename, timezone, verifyConsentOnDial, welcomePromptId, pseudonym, directory, enabled, disableStopRecording, oAuthProfileAssignmentFromExternalRole, countryCode, scrubOnDial, modifiedBy, promoCode, dispositionNotification, credentialsNonExpired, languageId, emailNotificationDomain, disableDisposition, avatar, allowClientTracing, userId, allowServerTracing, voicemailGreetingFilename, createdBy, disableEmailNotification, dialConfirmation, tenantId, reminderTime, username, id, defaultExtensionCampaignId, hideInCompanyDirectory, hrid, location, name, ringPhoneOnOffer, showWebrtcNotification
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Insert results into SQL Server
    for row in results:
        data_to_insert = (
            row.autoDispositionDelay,
            row.zipCode,
            row.modifiedAt,
            row.defaultAcdLoginStatus,
            row.recordingAnalysisPercentage,
            row.objectType,
            row.createdAt,
            row.hideInDirectory,
            row.password,
            row.emailAddress,
            row.userClientSettingsId,
            row.autoDispositionId,
            row.passwordModifiedAt,
            row.contentType,
            row.chatWelcome,
            row.dispositionDelay,
            row.callFailedNotification,
            row.dispositionId,
            row.allowAcdChanges,
            row.deviceInfo,
            row.deviceToken,
            row.firstName,
            row.deletedAt,
            row.edgeId,
            row.firebaseToken,
            row.phone,
            row.telephony,
            row.dob,
            row.referralLink,
            row.oAuthQueueAssignments,
            row.userProfileId,
            row.acdAutoAccept,
            row.lastName,
            row.extension,
            row.acdAutoLogin,
            row.role,
            row.gender,
            row.disableStartRecording,
            row.voicemailDropFilename,
            row.timezone,
            row.verifyConsentOnDial,
            row.welcomePromptId,
            row.pseudonym,
            row.directory,
            row.enabled,
            row.disableStopRecording,
            row.oAuthProfileAssignmentFromExternalRole,
            row.countryCode,
            row.scrubOnDial,
            row.modifiedBy,
            row.promoCode,
            row.dispositionNotification,
            row.credentialsNonExpired,
            row.languageId,
            row.emailNotificationDomain,
            row.disableDisposition,
            row.avatar,
            row.allowClientTracing,
            row.userId,
            row.allowServerTracing,
            row.voicemailGreetingFilename,
            row.createdBy,
            row.disableEmailNotification,
            row.dialConfirmation,
            row.tenantId,
            row.reminderTime,
            row.username,
            row.id,
            row.defaultExtensionCampaignId,
            row.hideInCompanyDirectory,
            row.hrid,
            row.location,
            row.name,
            row.ringPhoneOnOffer,
            row.showWebrtcNotification
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
