import os
import pyhdfs
from dotenv import load_dotenv

load_dotenv()

def get_db_env():
    return {
        "host": os.getenv("DATABASE_HOST"),
        "user": os.getenv("DATABASE_USER"),
        "password": os.getenv("DATABASE_PASSWORD"),
        "dbname": os.getenv("DATABASE_NAME"),
        "port": os.getenv("DATABASE_PORT"),
    }

def get_hdfs_connection():
    fs = pyhdfs.HdfsClient(hosts=f"{os.getenv('HDFS_HOST')}:{os.getenv('HDFS_PORT')}", user_name={os.getenv('HDFS_USER')})
    return fs

def get_nifi_env():
    return {
        "base_url": os.getenv("NIFI_BASE_URL"),
        "root": os.getenv("NIFI_ROOT"),
        "sql_template_id": os.getenv("SQL_TEMPLATE_ID"),
        "username": os.getenv("NIFI_USERNAME"),
        "password": os.getenv("NIFI_PASSWORD"),
    }