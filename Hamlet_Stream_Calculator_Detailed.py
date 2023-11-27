import argparse
import io
import json
import logging
import os
import pandas as pd
import re
from collections import Counter
from io import StringIO
from pyflink.common import SimpleStringSchema, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (DataTypes, EnvironmentSettings, FormatDescriptor,
                           Schema, StreamTableEnvironment, TableDescriptor,
                           TableEnvironment, udf)
from pyflink.table.expressions import col, lit

# 定义一个函数，用于移除文本中的标点符号
def remove_punctuation(text):
    # 使用正则表达式移除文本中的标点符号
    return re.sub(r'[^\w\s]','',text)

# 定义一个函数，用于计算文本中字节数
def count_bytes(text):
    # 返回文本中字节数
    return len(text.encode('utf-8'))

# 定义一个函数，用于计算文本中单词数
def count_words(text):
    # 将文本拆分成单词数组
    words = text.split()
    # 计算单词数
    result = dict(Counter(words))
    # 获取出现次数最多的单词
    max_word = max(result, key=result.get)
    # 返回文本中字节数、单词数、出现次数最多的单词以及出现次数
    return {'total_bytes': count_bytes(text), 'total_words': len(words), 'most_frequent_word': max_word, 'most_frequent_word_count': result[max_word]}

# 定义一个函数，用于从Kafka中读取数据
def read_from_kafka():
    # 获取StreamExecutionEnvironment实例
    env = StreamExecutionEnvironment.get_execution_environment()  
    # 添加FlinkKafkaConnector的jar包
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    # 创建FlinkKafkaConsumer实例
    kafka_consumer = FlinkKafkaConsumer(
        # 指定要读取的topic
        topics='hamlet', 
        # 指定反序列化器
        deserialization_schema= SimpleStringSchema('UTF-8'), 
        # 指定Kafka服务器的配置信息
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    
    # 从最早的记录开始读取
    kafka_consumer.set_start_from_earliest()
    # 将Kafka中的数据添加到Stream中
    stream_original_text = env.add_source(kafka_consumer)
    # 对Stream中的数据进行处理，移除文本中的标点符号
    stream_remove_punctuation = stream_original_text.map(lambda x: remove_punctuation(x))
    # 对Stream中的数据进行处理，计算文本中单词数
    stream_count_words = stream_remove_punctuation.map(lambda x: count_words(x))
    # 将处理后的数据打印出来
    stream_count_words.print()
    # 执行Stream中的任务
    env.execute()

# 调用read_from_kafka函数，从Kafka中读取数据
read_from_kafka()
