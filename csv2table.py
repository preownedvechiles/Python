import pandas as pd
import pyodbc




# SQL Server connection details
server = 'USPA01BISQL01'  # e.g., 'localhost' or '192.168.1.100'
database = 'EDW'
username = 'TL2020'
password = 'TEternal2021!'
table_name = 'import.testTab'
csv_file = 'C:\\testTab\\testCsv.csv'  # Path to your CSV file

# ODBC connection string
conn_str = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
"""

try:
    # Establish connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Connected to SQL Server successfully!")

    # Read CSV into a Pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # Clean column names
    df.columns = [col.strip().replace(' ', '_') for col in df.columns]

    # Construct SQL query dynamically
    placeholders = ', '.join(['?' for _ in df.columns])  # Generates (?, ?, ?, ...)
    columns = ', '.join(df.columns)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Insert data into SQL Server
    for row in df.itertuples(index=False, name=None):
        cursor.execute(sql, row)

    # Commit transaction
    conn.commit()
    print("Data successfully inserted into SQL Server!")

    # Close connection
    cursor.close()
    conn.close()

except Exception as e:
    print("Error:", e)
