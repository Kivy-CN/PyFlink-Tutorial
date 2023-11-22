import os
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

# 定义split函数，用于将字符串按照空格分割
def split(line):
    # 使用yield from语法，将line按照空格分割，并返回分割后的字符串
    yield from line.split()

# 定义read_from_kafka函数，用于从Kafka中读取数据
def read_from_kafka():
    # 获取StreamExecutionEnvironment实例
    env = StreamExecutionEnvironment.get_execution_environment()    
    # 添加FlinkKafkaConsumer的jar包
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    # 打印信息
    print("start reading data from kafka")
    # 创建FlinkKafkaConsumer实例，用于从Kafka中读取数据
    kafka_consumer = FlinkKafkaConsumer(
        topics='hamlet', # The topic to consume messages from
        deserialization_schema= SimpleStringSchema('UTF-8'), # The schema to deserialize messages
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} # The Kafka broker address and consumer group ID
    )
    # 从最早的记录开始读取数据
    kafka_consumer.set_start_from_earliest()
    # 将FlinkKafkaConsumer实例添加到StreamExecutionEnvironment实例中，并打印读取到的数据
    env.add_source(kafka_consumer).print()
    # 执行StreamExecutionEnvironment实例
    env.execute()

# 调用read_from_kafka函数
if __name__ == '__main__':
    # 设置日志级别
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    # 调用read_from_kafka函数
    read_from_kafka()