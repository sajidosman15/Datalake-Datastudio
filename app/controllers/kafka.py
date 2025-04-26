import os
import threading
from structlog import get_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

from app.models.connection import Connection
from app.config import get_hdfs_connection, get_kafka_server

logger = get_logger()

# Configure Spark to include Kafka packages
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,'
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
)

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
            "kafka.bootstrap.servers": get_kafka_server(),
            "subscribe": topic,
            "startingOffsets": "earliest"
        }
        # Create a Kafka DataFrame
        kafkaStream = spark.readStream \
            .format("kafka") \
            .options(**kafkaParams) \
            .load()
        # Convert the value column from bytes to string
        kafkaStream = kafkaStream.selectExpr("CAST(value AS STRING)")
        logger.info(f"Module:KafkaController. Finished getting stream from kafka topic: {topic}")
        return kafkaStream
    except Exception as e:
        logger.error(f"Module:KafkaController. Failed to get stream from kafka topic: {topic}. {str(e)}")
        return False

def store_kafka_stream(kafkaStream: DataFrame, topic_prefix: str, topic: str):
    '''
    This function store the Kafka data stream into Hadoop Distributed File System (HDFS)
    '''
    try:
        logger.info(f"Module:KafkaController. Start storring stream in Hadoop topic: {topic}")
        def store_in_hadoop(batch_df: DataFrame, batch_id: int):
            if not batch_df.isEmpty():
                # Convert each row to JSON string
                records = batch_df.toJSON().collect()
                content = "\n".join(records)
                # Generate unique file path for this batch
                file_path = f"/DataLake/{topic_prefix}/{topic}/{topic}.json"
                # Write to HDFS using pyhdfs
                fs = get_hdfs_connection()
                if fs.exists(file_path):
                    fs.delete(file_path)
                fs.create(file_path, content.encode('utf-8'))
            query.stop()

        query: StreamingQuery = kafkaStream.writeStream \
            .foreachBatch(store_in_hadoop) \
            .start()
        query.awaitTermination()
        logger.info(f"Module:KafkaController. Finished storring stream in Hadoop topic: {topic}")
        return True
    except Exception as e:
        logger.error(f"Module:KafkaController. Failed storring stream in Hadoop topic: {topic}. {str(e)}")
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
                stored = store_kafka_stream(kafkaStream, topic_prefix, topic)
                if not stored:
                    successfully_stored = False
            else:
                successfully_stored = False
            
        if successfully_stored:
            connection.update_state("Stored")
        else:
            connection.update_state("Failed")
        logger.info(f"Module:KafkaController. Finished transferring data Kafka to Hadoop. Process id:{connection.nifi_process_id}")
    except Exception as e:
        connection.update_state("Failed")
        logger.error(f"Module:KafkaController. Failed to transfer data Kafka to Hadoop. Process id:{connection.nifi_process_id}. {str(e)}")
    finally:
        if 'spark' in locals() and spark:
            spark.stop()

    