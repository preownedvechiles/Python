import pyodbc




# Define SQL Server connection parameters
server = 'rk'  # e.g., 'localhost' or '192.168.1.100'
database = 'EDW'
username = 'rk'
password = 'rk!'

# Define the ODBC connection string
conn_str = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
"""




# Use Windows Authentication
#conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
conn = pyodbc.connect(conn_str)
try:
    # Establish the connection
    conn = pyodbc.connect(conn_str)
    print("Connection successful!")

    # Create a cursor object
    cursor = conn.cursor()

    # Example query
    cursor.execute("select top 10 dim_date,date_key,day_of_week from edw.[import].[D_DATE]")

    insert_query = "INSERT INTO import.testpy (id, LastName, FirstName) VALUES (?, ?, ?)"
    for row in cursor.fetchall():
        print(f"dim_date: {row.dim_date}, date_key: {row.date_key},day_of_week: {row.day_of_week}")
        data_to_insert = (row.dim_date,row.date_key,row.day_of_week)
        cursor.execute(insert_query, data_to_insert)
        conn.commit()
        # Close the connection
    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")


