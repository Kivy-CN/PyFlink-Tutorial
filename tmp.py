import re
import argparse
import logging
import sys
import time
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
    Year_Begin =1900
    Year_End = 2023
    env = StreamExecutionEnvironment.get_execution_environment()    
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    kafka_consumer = FlinkKafkaConsumer(
        topics='data', 
        deserialization_schema= SimpleStringSchema('UTF-8'),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    kafka_consumer.set_start_from_earliest()
    
    data_stream =  env.add_source(kafka_consumer).map(lambda x: ' '.join(re.findall(r'\d+', x))).filter(lambda x: any([Year_Begin <= int(i) <= Year_End for i in x.split()])).map(lambda x:  [i for i in x.split() if Year_Begin <= int(i) <= Year_End][0])
    # data_stream.print()
    current_time = time.strftime("%Y%m%d-%H%M%S")
    table = t_env.from_data_stream(data_stream)
    # table.print_schema()
    sink = FileSink \
        .for_row_format('./', Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder()
                                 .with_part_prefix('prefix')
                                 .with_part_suffix('.txt')
                                 .build()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy()) \
        .build()
    table.execute_insert(sink).wait()
    print("table end reading data from kafka")
    env.execute()
    print("data stream end reading data from kafka")

if __name__ == '__main__':
    read_from_kafka()
