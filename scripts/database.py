import os
import psycopg2
from psycopg2 import sql

from dotenv import load_dotenv
from structlog import get_logger

logger = get_logger()
load_dotenv()

def get_db_env():
    return {
        "host": os.getenv("DATABASE_HOST"),
        "user": os.getenv("DATABASE_USER"),
        "password": os.getenv("DATABASE_PASSWORD"),
        "dbname": os.getenv("DATABASE_NAME"),
        "port": os.getenv("DATABASE_PORT"),
    }

def check_and_create_database():
    env = get_db_env()
    dbname = env["dbname"]
    
    try:
        # Attempt to connect to the target database
        conn = psycopg2.connect(
            dbname=dbname, host=env["host"], user=env["user"], 
            password=env["password"], port=env["port"]
        )
        logger.info(f"Database '{dbname}' already exists.")
        conn.close()
        return True
    except psycopg2.OperationalError:
        try:
            # Attempt to create a connection with default database
            conn = psycopg2.connect(
                dbname="postgres", host=env["host"], user=env["user"], 
                password=env["password"], port=env["port"]
            )
            conn.autocommit = True
            cur = conn.cursor()
            
            # Create the database correctly
            cur.execute(sql.SQL("CREATE DATABASE {}" ).format(sql.Identifier(dbname)))
            logger.info(f"Database '{dbname}' created successfully.")
            
            cur.close()
            conn.close()
            return True
        except Exception as e:
            if 'cur' in locals() and cur:
                cur.close()
            if 'conn' in locals() and conn:
                conn.close()
            logger.error(f"Error on creating database: {e}")
            return False

def create_connections_table():
    env = get_db_env()
    
    try:
        # Connect to the specified database
        conn = psycopg2.connect(
            dbname=env["dbname"], host=env["host"], user=env["user"], 
            password=env["password"], port=env["port"]
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Create the Connections table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS Connections (
            id SERIAL PRIMARY KEY,
            connection_name VARCHAR(255) NOT NULL,
            source_type VARCHAR(100) NOT NULL,
            connection_properties JSONB NOT NULL,
            state VARCHAR(50) NOT NULL,
            nifi_process_id VARCHAR(255),
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
        
        cur.execute(create_table_query)
        logger.info("Table 'Connections' created successfully.")
        
        cur.close()
        conn.close()
    except Exception as e:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        logger.error(f"Failed to create table 'Connections' : {e}")

def create_datasets_table():
    env = get_db_env()
    
    try:
        # Connect to the specified database
        conn = psycopg2.connect(
            dbname=env["dbname"], host=env["host"], user=env["user"], 
            password=env["password"], port=env["port"]
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Create the Datasets table
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS Datasets (
            id SERIAL PRIMARY KEY,
            dataset_name VARCHAR(255) NOT NULL,
            api_version VARCHAR(100) NOT NULL,
            dataset_owner VARCHAR(100) NOT NULL,
            visibility VARCHAR(50) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            dashboard_url VARCHAR(1000),
            connection_id INT NOT NULL,
            FOREIGN KEY (connection_id) REFERENCES Connections(id),
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''
        
        cur.execute(create_table_query)
        logger.info("Table 'Datasets' created successfully.")
        
        cur.close()
        conn.close()
    except Exception as e:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        logger.error(f"Failed to create table 'Datasets' : {e}")

if __name__ == "__main__":
    database = check_and_create_database()

    if database:
        create_connections_table()
        create_datasets_table()
