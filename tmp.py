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
    env = StreamExecutionEnvironment.get_execution_environment()    
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    kafka_consumer = FlinkKafkaConsumer(
        topics='data', 
        deserialization_schema= SimpleStringSchema('UTF-8'),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    kafka_consumer.set_start_from_earliest()
    
    data_stream =  env.add_source(kafka_consumer).map(lambda x: ' '.join(re.findall(r'\d+', x))).filter(lambda x: any([Year_Begin <= int(i) <= Year_End for i in x.split()])).map(lambda x:  [i for i in x.split() if Year_Begin <= int(i) <= Year_End][0])
    data_stream.print()
    env.execute()
    print('Is it Here yet?')

if __name__ == '__main__':
    read_from_kafka()
