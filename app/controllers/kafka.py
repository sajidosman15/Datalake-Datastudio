import os
import threading
from structlog import get_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from app.models.connection import Connection
from app.controllers.hadoop import get_hdfs_path
from app.config import config

logger = get_logger()

# Configure Spark to include Kafka packages
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,'
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
)
os.environ['HADOOP_USER_NAME'] = config.hadoop.user

def run_kafka_to_hadoop_thread(connection: Connection):
    try:
        threading.Thread(
            target = store_data_kafka_to_hadoop,
            args = (connection,),
            daemon = True
        ).start()
        return True
    except Exception as e:
        return False


def initialize_spark() -> SparkSession:
    spark = SparkSession.builder.appName("KafkaStreamProcessor").getOrCreate()
    return spark

def get_dataset_list(connection: Connection):
    '''
    Select and extract the relevant datasets corresponding to each data source type
    '''
    if connection.source_type == "Microsoft SQL Server":
        return connection.connection_properties['tables']
    
def get_topic_prefix(connection: Connection):
    '''
    Return the topic prefix based on the source type.
    '''
    if connection.source_type == "Microsoft SQL Server":
        return connection.connection_properties['db_name']

def get_kafka_stream(topic: str, spark: SparkSession):
    '''
    This function retrieve data from a Kafka stream for a specific topic.
    '''
    try:
        logger.info(f"Module:KafkaController. Start getting stream from kafka topic: {topic}")
        kafkaParams = {
            "kafka.bootstrap.servers": config.kafka.get_url(),
            "subscribe": topic,
            "startingOffsets": "earliest",
            "failOnDataLoss": "false"
        }
        # Create a Kafka DataFrame
        kafkaStream = spark.readStream \
            .format("kafka") \
            .options(**kafkaParams) \
            .load() \
            .selectExpr("CAST(value AS STRING)")

        logger.info(f"Module:KafkaController. Finished getting stream from kafka topic: {topic}")
        return kafkaStream
    except Exception as e:
        logger.error(f"Module:KafkaController. Failed to get stream from kafka topic: {topic}. {str(e)}")
        return False

def store_kafka_stream(kafkaStream: DataFrame, topic_prefix: str, dataset_name: str):
    '''
    This function store the Kafka data stream into Hadoop Distributed File System (HDFS)
    '''
    try:
        logger.info(f"Module:KafkaController. Start storring stream in Hadoop dataset: {dataset_name}")
        def store_in_hadoop(batch_df: DataFrame, batch_id: int):
            if not batch_df.isEmpty():
                # Store the batch in the specified folder
                folder_path = f"/DataLake/{topic_prefix}/{dataset_name}"
                hdfs_path = get_hdfs_path(folder_path)
                batch_df.write.mode("overwrite").json(hdfs_path)

                # Rename the randomly generated file name to actual file name.
                dfs = config.hadoop.get_connection()
                old_file_name = f"{folder_path}/{dfs.list_status(folder_path)[1]["pathSuffix"]}"
                new_file_name = f"{folder_path}/{dataset_name}.json"
                dfs.rename(old_file_name,new_file_name)

            query.stop()

        query: StreamingQuery = kafkaStream.writeStream \
            .foreachBatch(store_in_hadoop) \
            .start()
        query.awaitTermination()
        logger.info(f"Module:KafkaController. Finished storring stream in Hadoop dataset: {dataset_name}")
        return True
    except Exception as e:
        logger.error(f"Module:KafkaController. Failed storring stream in Hadoop dataset: {dataset_name}. {str(e)}")
        return False
    finally:
        if 'query' in locals() and query:
            query.stop()

def store_data_kafka_to_hadoop(connection: Connection):
    '''
    This function is a data ingestion pipeline that continuously retrieves streaming data from Apache Kafka 
    and stores it into Hadoop Distributed File System (HDFS) for all datasets involved in the process.
    '''
    try:
        logger.info(f"Module:KafkaController. Start transferring data Kafka to Hadoop. Process id:{connection.nifi_process_id}")
        successfully_loaded = True
        successfully_stored = True
        connection.update_state("Storing")
        dataset_list = get_dataset_list(connection)
        topic_prefix = get_topic_prefix(connection)

        # Initialize spark session
        spark = initialize_spark()
        for dataset_name in dataset_list:
            topic = f"{topic_prefix}{dataset_name}"
            kafkaStream = get_kafka_stream(topic, spark)
            if kafkaStream:
                stored = store_kafka_stream(kafkaStream, topic_prefix, dataset_name)
                if not stored:
                    successfully_stored = False
            else:
                successfully_loaded = False
            
        if successfully_stored:
            connection.update_state("Stored")
        elif successfully_loaded:
            connection.update_state("Loaded")
        else:
            connection.update_state("Failed")
            
        logger.info(f"Module:KafkaController. Finished transferring data Kafka to Hadoop. Process id:{connection.nifi_process_id}")
    except Exception as e:
        connection.update_state("Failed")
        logger.error(f"Module:KafkaController. Failed to transfer data Kafka to Hadoop. Process id:{connection.nifi_process_id}. {str(e)}")
    finally:
        if 'spark' in locals() and spark:
            spark.stop()

    