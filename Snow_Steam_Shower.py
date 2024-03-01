import platform
import os
import argparse
import csv
import io
import logging
import sys
from typing import Iterable

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
from datetime import datetime
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.common import SimpleStringSchema

# 定义一个函数parse_csv_old，用于解析csv文件
def parse_csv_old(x):
    # 使用csv模块的reader函数读取csv文件
    result = csv.reader(io.StringIO(x))    
    # 返回csv文件的第一行
    return next(result)

# 定义一个函数parse_csv，用于解析csv文件
def parse_csv(x):
    # 将x中的[b'替换为空字符
    x = x.replace("[b'", "")
    # 将x中的\\n']替换为空字符
    x = x.replace("\\n']", "")
    # 使用csv模块的reader函数读取csv文件
    result = csv.reader(io.StringIO(x))
    # 返回csv文件的第一行
    return next(result)

# 定义一个函数，用于计算传入数据的行数
def count_rows(data):
    # 计算传入数据的行数
    row_count = len(data)
    # 计算传入数据的类型
    type_count = type(data)
    # 打印出传入数据的行数和类型
    print(f"Received {row_count} rows of {type_count} data.")
    # 返回传入数据
    return data 

# 定义一个函数，用于解析元组
def parse_tuple(x):
    
    # 打印出传入数据的第一个元素的类型、第二个元素的类型和第一个元素的长度
    print(f"x[0] type is {type(x[0])}",f"x[0][1] type is {type(x[0][1])}",f"x[0] len is {len(x[0])}")
    try:
        # 尝试使用datetime.strptime函数将传入数据的第一个元素转换为时间戳，并将其第二个元素转换为float类型
        return (datetime.strptime(str(x[0][0]), "%Y-%m-%d %H:%M:%S").timestamp(), float(x[0][1]))
    except ValueError:
        # 如果转换失败，则打印出传入数据的值，并返回None
        logging.error(f"Failed to parse tuple: {x}")
        return None

# 定义一个函数read_from_kafka，用于从Kafka读取数据
def read_from_kafka():
    # 获取StreamExecutionEnvironment实例
    env = StreamExecutionEnvironment.get_execution_environment()    
    # 添加flink-sql-connector-kafka-3.1-SNAPSHOT.jar包
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    # 打印信息
    print("start reading data from kafka")
    # 创建一个FlinkKafkaConsumer实例，用于从Kafka读取数据
    kafka_consumer = FlinkKafkaConsumer(
        topics='snow_load', # The topic to consume messages from
        deserialization_schema= SimpleStringSchema('UTF-8'), # The schema to deserialize messages
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} # The Kafka broker address and consumer group ID
    )
    # 从最早的记录开始读取数据
    kafka_consumer.set_start_from_earliest()
    # 将kafka_consumer添加到StreamExecutionEnvironment中
    stream = env.add_source(kafka_consumer)
    # 将stream中的每一条数据解析为csv文件
    parsed_stream = stream.map(parse_csv)
    # 打印解析后的数据
    parsed_stream.print()
    # 执行StreamExecutionEnvironment
    env.execute()

# 调用函数read_from_kafka
if __name__ == '__main__':
    read_from_kafka()