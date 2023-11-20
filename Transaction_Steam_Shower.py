import os
# Get current absolute path
current_file_path = os.path.abspath(__file__)
# Get current dir path
current_dir_path = os.path.dirname(current_file_path)
# Change into current dir path
os.chdir(current_dir_path)
output_path = current_dir_path

import argparse
import csv
import re
import io
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

def parse_csv(x):
    result = csv.reader(io.StringIO(x))
    for items in result:
        # print(items)
        pass
    return next(result)

def read_from_kafka():
    env = StreamExecutionEnvironment.get_execution_environment()    
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    kafka_consumer = FlinkKafkaConsumer(
        topics='transaction', # The topic to consume messages from
        deserialization_schema= SimpleStringSchema('UTF-8'), # The schema to deserialize messages
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} # The Kafka broker address and consumer group ID
    )
    kafka_consumer.set_start_from_earliest()
    stream = env.add_source(kafka_consumer)
    # parsed_stream = stream.map(lambda x: next(csv.reader(io.StringIO(x))))
    parsed_stream = stream.map(parse_csv)
    parsed_stream.print()
    env.execute()

if __name__ == '__main__':
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    read_from_kafka()
