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
from math import radians, cos, sin, asin, sqrt

# 定义开始年份和结束年份
Year_Begin =1999
Year_End = 2023

def extract_numbers(x):
    return ' '.join(re.findall(r'\d+', x))

def parse_csv(x):    
    x = x.replace("[b'", "")
    x = x.replace("\n']", "")
    x = x.replace("\\n']", "")
    x = x.replace("\\r']", "")
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
    

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r

def calculate_distance(x):
    data =x[0]
    """
    Extract the start and end coordinates from the data and calculate the distance
    """
    start_lat = float(data[7])
    start_lon = float(data[8])
    end_lat = float(data[9])
    end_lon = float(data[10].strip('\\r'))

    return haversine(start_lon, start_lat, end_lon, end_lat)


def filter_years(x):
    return any([Year_Begin <= int(i) <= Year_End for i in x.split()])

def map_years(x):
    return [i for i in x.split() if Year_Begin <= int(i) <= Year_End][0]

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
    # ds = ds.map(extract_numbers)
    # ds = ds.filter(filter_years)
    # ds = ds.map(map_years)
    ds = ds.map(parse_csv)
    ds = ds.map(calculate_distance)
    ds.print()
    env.execute()

if __name__ == '__main__':
    read_from_kafka()