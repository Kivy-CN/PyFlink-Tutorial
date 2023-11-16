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
from io import StringIO
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
    Year_Begin =1999
    Year_End = 2023
    # Create a Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()    

    # Add the Flink SQL Kafka connector jar file to the classpath
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")

    # Print a message to indicate that data reading from Kafka has started
    print("start reading data from kafka")

    # Create a Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='data', # The topic to consume messages from
        deserialization_schema= SimpleStringSchema('UTF-8'), # The schema to deserialize messages
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} # The Kafka broker address and consumer group ID
    )

    # Start reading messages from the earliest offset
    kafka_consumer.set_start_from_earliest()

    # 重定向sys.stdout到一个io.StringIO对象
    output = StringIO()
    sys.stdout = output

    # Add the Kafka consumer as a source to the Flink execution environment and print the messages to the console
    env.add_source(kafka_consumer).map(lambda x: ' '.join(re.findall(r'\d+', x))).filter(lambda x: any([Year_Begin <= int(i) <= Year_End for i in x.split()])).map(lambda x:  [i for i in x.split() if Year_Begin <= int(i) <= Year_End][0]).print()

    # 恢复sys.stdout
    sys.stdout = sys.__stdout__

    # 输出捕获的内容
    print(output.getvalue())
    
    # submit for execution
    env.execute()

if __name__ == '__main__':
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    read_from_kafka()
