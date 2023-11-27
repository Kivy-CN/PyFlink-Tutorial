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

# 定义一个beep函数，用于发出哔声，根据当前操作系统不同，使用不同的库
def beep():
    if platform.system() == "Windows":
        import winsound
        winsound.Beep(440, 1000)
    elif platform.system() == "Linux":
        os.system("beep")
    else:
        print("Unsupported platform")

# 定义一个parse_csv函数，用于解析csv文件，并返回解析后的结果
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

# 定义一个count_rows函数，用于计算data中行数和类型，并打印出来
def count_rows(data):
    row_count = len(data)
    type_count = type(data)
    print(f"Received {row_count} rows of {type_count} data.")
    return data 

# 定义一个check_data函数，用于检查data中第一行的第四个元素是否大于5000，如果大于5000，则发出哔声，并打印出来
def check_data(data):
    try:
        if int(data[0][3]) >= 5000:
            beep()
            print(f"data[0][3] is {(data[0][3])}",f" Larger than 5000!\n")
        return int(data[0][3]) >= 5000
    except ValueError:
        pass
        
    

# 定义一个函数，用于绘制数据流的折线图
def plot_data_stream(data_item):
    # 创建一个新的图形对象
    fig = plt.figure()
    # 设置图形的标题和坐标轴标签
    plt.title("Data Stream of the Fourth Column")
    plt.xlabel("Time")
    plt.ylabel("Value")
    # 创建一个子图对象，用于绘制折线图
    ax = fig.add_subplot(111)
    # 使用plot方法绘制数据流的折线图，传入x和y参数，以及其他可选参数
    ax.plot(data_item, label="Value")
    # 设置折线图的颜色和线宽
    ax.plot(data_item, color="blue", linewidth=2)
    # 设置折线图的标签和图例位置
    ax.legend(loc="upper left")
    # 显示图形
    plt.show()



# 定义一个函数parse_tuple，用于解析元组，参数x为元组
def parse_tuple(x):
    try:
        # 返回元组中的第一个元素，转换为字符串，第二个元素，转换为字符串，第三个元素，转换为整数，第四个元素，转换为整数，第五个元素，转换为字符串，第六个元素，转换为字符串
        return (str(x[0][0]), str(x[0][1]), int(x[0][2]), int(x[0][3]), str(x[0][4]), str(x[0][5]))
    except ValueError:
        # 如果解析失败，打印错误信息
        logging.error(f"Failed to parse tuple: {x}")
        return None

# 定义一个函数read_from_kafka，用于从Kafka中读取数据
def read_from_kafka():
    # 创建一个参数解析器
    parser = argparse.ArgumentParser()
    # 添加一个参数，用于指定输出文件路径
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')
    # 获取参数列表
    argv = sys.argv[1:]
    # 解析参数列表
    known_args, _ = parser.parse_known_args(argv)
    # 获取输出文件路径
    output_path = known_args.output
    # 获取Flink运行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    # 设置并行度为1
    env.set_parallelism(1)  
    # 添加Kafka连接器
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    # 打印信息
    print("start reading data from kafka")
    # 创建一个FlinkKafkaConsumer，用于从Kafka中读取数据
    kafka_consumer = FlinkKafkaConsumer(
        # 指定要读取的topic
        topics='transaction',
        # 指定反序列化方式
        deserialization_schema= SimpleStringSchema('UTF-8'), 
        # 指定Kafka的配置信息
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    # 从最早的记录开始读取
    kafka_consumer.set_start_from_earliest()
    # 从Kafka中读取数据
    stream = env.add_source(kafka_consumer)
    # 将读取的数据进行解析
    parsed_stream = stream.map(parse_csv)
    # 过滤掉不符合条件的数据
    data_stream = parsed_stream.filter(check_data)
    # 定义输出流
    data_stream.print()
    env.execute()

if __name__ == '__main__':
    read_from_kafka()