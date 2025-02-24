import psycopg2
import pandas as pd

# Database connection parameters
SOURCE_DB = {
    'host': 'localhost',
    'dbname': 'airflow',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

TARGET_DB = {
    'host': 'localhost',
    'dbname': 'airflow',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def extract():
    """Extract data from the source database."""
    try:
        conn = psycopg2.connect(**SOURCE_DB)
        query = "SELECT id, name, age, salary FROM employees;"  # Modify query as needed
        df = pd.read_sql_query(query, conn)
        conn.close()
        print("Data extracted successfully!")
        return df
    except Exception as e:
        print(f" Extraction failed: {e}")
        return None

def transform(df):
    """Transform the extracted data."""
    df.dropna(inplace=True)  # Remove NULL values
    df['name'] = df['name'].str.upper()  # Convert names to uppercase
    df['salary'] = df['salary'] * 1.1  # Increase salary by 10%
    print("Data transformed successfully!")
    return df

def load(df):
    """Load transformed data into the target database."""
    try:
        conn = psycopg2.connect(**TARGET_DB)
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO processed_employees (id, name, age, salary)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE 
                SET name = EXCLUDED.name, age = EXCLUDED.age, salary = EXCLUDED.salary;
            """, (row['id'], row['name'], row['age'], row['salary']))
        
        conn.commit()
        conn.close()
        print("Data loaded successfully!")
    except Exception as e:
        print(f" Loading failed: {e}")

# Run ETL pipeline
if __name__ == "__main__":
    data = extract()
    if data is not None:
        transformed_data = transform(data)
        load(transformed_data)
