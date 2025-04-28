import os
import psycopg2
from pyhdfs import HdfsClient
from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv()

class Hadoop:
    def __init__(self):
        self.host = os.getenv('HDFS_HOST')
        self.namenode_port = os.getenv('HDFS_NAMENODE_PORT')
        self.web_port = os.getenv('HDFS_WEB_PORT')
        self.user = os.getenv('HDFS_USER')

    def get_connection(self):
        return HdfsClient(hosts=f"{self.host}:{self.web_port}", user_name={self.user})

class NiFi:
    def __init__(self):
        self.host = os.getenv('NIFI_HOST')
        self.port = os.getenv('NIFI_PORT')
        self.username = os.getenv('NIFI_USERNAME')
        self.password = os.getenv('NIFI_PASSWORD')
        self.root = os.getenv('NIFI_ROOT')
        self.sql_template_id = os.getenv('SQL_TEMPLATE_ID')

    def get_api_url(self):
        return f"https://{self.host}:{self.port}/nifi-api"

class Kafka:
    def __init__(self):
        self.host = os.getenv('KAFKA_HOST')
        self.port = os.getenv('KAFKA_PORT')

    def get_url(self):
        return f"{self.host}:{self.port}"
    
class ClickHouse:
    def __init__(self):
        self.host = os.getenv('CLICKHOUSE_HOST')
        self.port = os.getenv('CLICKHOUSE_PORT')
        self.username = os.getenv('CLICKHOUSE_USERNAME')
        self.password = os.getenv('CLICKHOUSE_PASSWORD')

    def get_connection(self, database: str):
        return get_client(host=self.host, port=self.port, username=self.username, password=self.password, database=database)

class Database:
    def __init__(self):
        self.host = os.getenv('DATABASE_HOST')
        self.port = os.getenv('DATABASE_PORT')
        self.username = os.getenv('DATABASE_USER')
        self.password = os.getenv('DATABASE_PASSWORD')
        self.dbname = os.getenv('DATABASE_NAME')

    def get_connection(self):
        return psycopg2.connect(dbname=self.dbname, host=self.host, user=self.username, password=self.password, port=self.port)
    
class APIService:
    def __init__(self):
        self.host = os.getenv('API_SERVER_URL')
        self.port = os.getenv('API_SERVER_PORT')

    def get_api_url(self):
        return f"http://{self.host}:{self.port}"

class Config:
    def __init__(self):
        self.hadoop = Hadoop()
        self.nifi = NiFi()
        self.kafka = Kafka()
        self.clickhouse = ClickHouse()
        self.database = Database()
        self.api_service = APIService()
        self.debug_mode = False

config = Config()