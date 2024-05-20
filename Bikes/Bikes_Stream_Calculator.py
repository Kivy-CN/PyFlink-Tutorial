import re
import argparse
import logging
import sys
import csv
import io
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

import math

# 定义开始年份和结束年份
Year_Begin =1999
Year_End = 2023

def extract_numbers(x):
    return ' '.join(re.findall(r'\d+', x))

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


def filter_years(x):
    return any([Year_Begin <= int(i) <= Year_End for i in x.split()])

def map_years(x):
    return [i for i in x.split() if Year_Begin <= int(i) <= Year_End][0]

def calculate_distance(row):
    row = parse_csv(row)
    print(row)
    # Split the row into columns
    columns = row.split(',')
    print(len(columns))

    # Extract the relevant columns
    start_lat, start_long, end_lat, end_long = map(float, [columns[-4], columns[-3], columns[-2], columns[-1]])

    # Convert latitude and longitude from degrees to radians
    start_lat, start_long, end_lat, end_long = map(math.radians, [start_lat, start_long, end_lat, end_long])

    # Haversine formula
    dlon = end_long - start_long
    dlat = end_lat - start_lat
    a = math.sin(dlat/2)**2 + math.cos(start_lat) * math.cos(end_lat) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    # Radius of earth in kilometers. Use 3956 for miles
    r = 6371

    # Calculate the result
    distance = c * r

    # Append the distance to the row and return it
    return row + ',' + str(distance)

def read_from_kafka():
    # 获取流环境
    env = StreamExecutionEnvironment.get_execution_environment()    
    # 添加jar包
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    # 创建kafka消费者
    kafka_consumer = FlinkKafkaConsumer(
        topics='bikes', 
        deserialization_schema= SimpleStringSchema('UTF-8'),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    # 从最早开始读取数据
    kafka_consumer.set_start_from_earliest()
    # 创建输出流
    output = StringIO()
    sys.stdout = output
    # 添加源，并过滤出指定年份的数据
    ds = env.add_source(kafka_consumer)
    ds = ds.map(extract_numbers)
    ds = ds.filter(filter_years)
    ds = ds.map(map_years)
    ds = ds.map(calculate_distance)
    ds.print()
    env.execute()

if __name__ == '__main__':
    read_from_kafka()