import os
import argparse
import logging
import re
import sys

import numpy as np
import pandas as pd

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, FileSource, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.csv import CsvRowDeserializationSchema, CsvRowSerializationSchema
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import StreamTableEnvironment


# 定义一个函数，从Kafka读取数据
def read_from_kafka():
    # 获取当前的StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()    
    # 添加Flink Kafka连接器JAR包
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    # 打印信息
    print("start reading data from kafka")
    # 创建一个FlinkKafkaConsumer，用于从Kafka读取数据
    kafka_consumer = FlinkKafkaConsumer(
        topics='bikes', # The topic to consume messages from
        deserialization_schema= SimpleStringSchema('UTF-8'),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} # The Kafka broker address and consumer group ID
    )

    # 从最早的记录开始读取数据
    kafka_consumer.set_start_from_earliest()
    # 将Kafka消费者添加到StreamExecutionEnvironment，并打印输出
    env.add_source(kafka_consumer).print()
    # 执行StreamExecutionEnvironment
    env.execute()

# 调用函数
if __name__ == '__main__':
    read_from_kafka()
