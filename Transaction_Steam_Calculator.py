import os
# # Get current absolute path
# current_file_path = os.path.abspath(__file__)
# # Get current dir path
# current_dir_path = os.path.dirname(current_file_path)
# # Change into current dir path
# os.chdir(current_dir_path)
# output_path = current_dir_path

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
    x = x.replace("[b'", "")
    x = x.replace("\n']", "")
    x = x.replace("\\n']", "")
    result = csv.reader(io.StringIO(x))
    parsed_result = []
    for item in result:
        parsed_item = []
        for element in item:
            try:
                parsed_element = int(element)
            except ValueError:
                parsed_element = element
            parsed_item.append(parsed_element)
        parsed_result.append(parsed_item)
    return parsed_result

def count_rows(data):
    row_count = len(data)
    type_count = type(data)
    print(f"Received {row_count} rows of {type_count} data.")
    return data 

def find_max_min(data):
    transpose_data = list(zip(*data))
    col_4 = transpose_data[3]
    print(f"column 4 is: {col_4}")
    return data

def read_from_kafka():
    env = StreamExecutionEnvironment.get_execution_environment()    
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    kafka_consumer = FlinkKafkaConsumer(
        topics='transaction',
        deserialization_schema= SimpleStringSchema('UTF-8'), 
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    kafka_consumer.set_start_from_earliest()
    stream = env.add_source(kafka_consumer)
    parsed_stream = stream.map(parse_csv)
    # parsed_stream.print()
    count_stream = parsed_stream.map(count_rows)
    # count_stream.print()
    max_min_stream = count_stream.map(find_max_min)
    max_min_stream.print()
    env.execute()

if __name__ == '__main__':
    read_from_kafka()
