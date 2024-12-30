import duckdb
import pandas as pd
import sys
import os

def create_table_if_not_exists(db_connection):
    """Creates the company_flights table if it doesn't already exist."""
    db_connection.execute('''
        CREATE TABLE IF NOT EXISTS company_flights (
            employee_id INTEGER,
            department TEXT,
            route_index INTEGER,
            flight_date TEXT
        )
    ''')

def process_csv(file_path, db_connection):
    """Processes the CSV file and inserts its content into the database."""
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' does not exist.")
        sys.exit(1)

    try:
        # Load the CSV file into a Pandas DataFrame
        df = pd.read_csv(file_path)

        # Validate headers
        expected_columns = ['employee_id', 'department', 'route_index', 'flight_date']
        if list(df.columns) != expected_columns:
            print(f"Error: Expected columns {expected_columns}, but got {list(df.columns)}")
            sys.exit(1)

        # Drop duplicate rows
        df = df.drop_duplicates()

        # Insert data into the database
        db_connection.register("temp_table", df)
        db_connection.execute('''
            INSERT INTO company_flights
            SELECT * FROM temp_table
            WHERE NOT EXISTS (
                SELECT 1 FROM company_flights cf
                WHERE cf.employee_id = temp_table.employee_id
                  AND cf.department = temp_table.department
                  AND cf.route_index = temp_table.route_index
                  AND cf.flight_date = temp_table.flight_date
            )
        ''')

        print(f"Successfully processed {len(df)} records from the file.")
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 flights_upload.py PATH_TO_FILE")
        sys.exit(1)

    file_path = sys.argv[1]
    db_path = 'flights.duckdb'

    # Connect to the DuckDB database
    db_connection = duckdb.connect(database=db_path)

    try:
        create_table_if_not_exists(db_connection)
        process_csv(file_path, db_connection)
    finally:
        db_connection.close()

if __name__ == "__main__":
    main()
