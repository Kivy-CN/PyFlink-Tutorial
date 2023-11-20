import os
# Get current absolute path
current_file_path = os.path.abspath(__file__)
# Get current dir path
current_dir_path = os.path.dirname(current_file_path)
# Change into current dir path
os.chdir(current_dir_path)
output_path = current_dir_path


import re
import argparse
import logging
import sys
import numpy as np 
import pandas as pd
from pyflink.table import StreamTableEnvironment
from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.common import Types, SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer

def split(line):
    yield from line.split()

def read_from_kafka():
    # Create a Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()    

    # Add the Flink SQL Kafka connector jar file to the classpath
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")

    # Print a message to indicate that data reading from Kafka has started
    print("start reading data from kafka")

    # Credit Card Number	Name	ID Number	Amount	Direction	Transaction Time
    # 4.59084E+15	Anne Jordan	86112888	6954	out	2022/12/30 0:00
    # 4.43847E+15	Colin Thomas	470402943	4150	out	2022/12/30 0:00

    # 定义类型信息，类型为STRING和INT等等
    type_info = Types.ROW([Types.INT(), Types.STRING(),Types.INT(),Types.INT(),Types.STRING(),Types.STRING()])
    # 创建反序列化schema，类型为INT和STRING
    deserialization_schema = CsvRowDeserializationSchema.Builder(type_info).build()

    # Create a Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='data', # The topic to consume messages from
        # deserialization_schema= SimpleStringSchema('UTF-8'),
        deserialization_schema=deserialization_schema, # The schema to deserialize messages
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} # The Kafka broker address and consumer group ID
    )

    # Start reading messages from the earliest offset
    kafka_consumer.set_start_from_earliest()

    # Add the Kafka consumer as a source to the Flink execution environment and print the messages to the console
    env.add_source(kafka_consumer).print()
    # submit for execution
    env.execute()

if __name__ == '__main__':
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    read_from_kafka()
