import pyodbc

# Function to get table names from SQL Server
def get_sql_server_table_names(db_url, db_name, db_username, db_password):
    try:
        # Construct the connection string
        conn_str = f"DRIVER={{SQL Server}};SERVER={db_url};DATABASE={db_name};UID={db_username};PWD={db_password}"
        
        # Establish connection
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Fetch all table names
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Close connection
        conn.close()
        return tables

    except Exception as e:
        return False