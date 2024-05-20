import os
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
        topics='transaction', # The topic to consume messages from
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