import platform
import os
import argparse
import csv
import io
import logging
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from typing import Iterable
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

# 定义一个beep函数，根据不同的操作系统，播放不同的声音
def beep():
    # 如果是Windows系统
    if platform.system() == "Windows":
        # 导入winsound模块
        import winsound
        # 播放440Hz的音调，持续1000ms
        winsound.Beep(440, 1000)
    # 如果是Linux系统
    elif platform.system() == "Linux":
        # 播放beep命令
        os.system("beep")
    # 如果是其他系统
    else:
        # 打印不支持的平台
        print("Unsupported platform")

# 定义一个parse_csv函数，用于解析csv文件
def parse_csv(x):    
    # 将x中的[b'替换为空
    x = x.replace("[b'", "")
    # 将x中的\n']替换为空
    x = x.replace("\n']", "")
    # 将x中的\\n']替换为空
    x = x.replace("\\n']", "")
    # 将x转换为csv格式
    result = csv.reader(io.StringIO(x))
    # 创建一个空列表，用于存放解析后的结果
    parsed_result = []
    # 遍历result中的每一项
    for item in result:
        # 创建一个空列表，用于存放解析后的每一项
        parsed_item = []
        # 遍历item中的每一项
        for element in item:
            # 尝试将element转换为整数
            try:
                parsed_element = int(element)
            # 如果转换失败，则将element的值赋给parsed_element
            except ValueError:
                parsed_element = element
            # 将parsed_element添加到parsed_item中
            parsed_item.append(parsed_element)
        # 将parsed_item添加到parsed_result中
        parsed_result.append(parsed_item)
    # 返回解析后的结果
    return parsed_result

# 定义一个count_rows函数，用于计算data中行数
def count_rows(data):
    # 计算data中行数
    row_count = len(data)
    # 获取data的类型
    type_count = type(data)
    # 打印data中行数和类型
    print(f"Received {row_count} rows of {type_count} data.")
    # 返回data
    return data 

# 定义一个check_data函数，用于检查data中每一行的数据
def check_data(data):
    # 检查data中第一行的数据是否大于0.5
    if abs(float(data[0][1])) >= 0.5:
        # 如果大于0.5，则播放beep函数
        beep()
        # 打印data中第一行的数据和ABS值
        print(f"data at {data[0][0]} is {(data[0][1])}",f" ABS Larger than 0.5!\n")

    # 返回data
    return data

# 定义一个parse_tuple函数，用于解析tuple
def parse_tuple(x):
    # 尝试将x中的第一个元素转换为字符串，第二个元素转换为浮点数
    try:
        return (str(x[0][0]), float(x[0][1]))
    # 如果转换失败，则打印错误信息
    except ValueError:
        logging.error(f"Failed to parse tuple: {x}")
        # 返回None
        return None

# 定义一个read_from_kafka函数，用于从kafka中读取数据
def read_from_kafka():
    # 获取StreamExecutionEnvironment实例
    env = StreamExecutionEnvironment.get_execution_environment()
    # 添加jars
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    # 打印提示信息
    print("start reading data from kafka")
    # 创建一个FlinkKafkaConsumer实例，用于从kafka中读取数据
    kafka_consumer = FlinkKafkaConsumer(
        topics='building',
        deserialization_schema= SimpleStringSchema('UTF-8'), 
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
        
    # 将kafka_consumer添加到StreamExecutionEnvironment中
    stream = env.add_source(kafka_consumer)
    # 将stream中的每一行数据转换为csv格式
    parsed_stream = stream.map(parse_csv)

    # 将parsed_stream中的每一行数据传入check_data函数，检查数据是否符合要求
    data_stream = parsed_stream.map(check_data)

    # 将data_stream中的数据打印到标准输出中
    print("Printing result to stdout.")
    data_stream.print()

    # 执行StreamExecutionEnvironment
    env.execute()

if __name__ == '__main__':
    # 调用read_from_kafka函数，从kafka中读取数据
    read_from_kafka()