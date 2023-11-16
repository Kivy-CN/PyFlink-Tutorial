import argparse
import io
import json
import logging
import os
import re
from collections import Counter
from io import StringIO
import pandas as pd
from pyflink.common import SimpleStringSchema, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (DataTypes, EnvironmentSettings, FormatDescriptor,
                           Schema, StreamTableEnvironment, TableDescriptor,
                           TableEnvironment, udf)
from pyflink.table.expressions import col, lit
from pyflink.common.typeinfo import Types


def remove_punctuation(text):
    return re.sub(r'[^\w\s]','',text)

def count_bytes(text):
    return len(text.encode('utf-8'))

def count_words(text):
    words = text.split()
    result = dict(Counter(words))
    max_word = max(result, key=result.get)
    return {'total_bytes': count_bytes(text), 'total_words': len(words), 'most_frequent_word': max_word, 'most_frequent_word_count': result[max_word]}

def read_from_kafka():
    env = StreamExecutionEnvironment.get_execution_environment()  
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    kafka_consumer = FlinkKafkaConsumer(
        topics='hamlet', 
        deserialization_schema= SimpleStringSchema('UTF-8'), 
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    kafka_consumer.set_start_from_earliest()
    stream_original_text = env.add_source(kafka_consumer)
    stream_remove_punctuation = stream_original_text.map(lambda x: remove_punctuation(x))
    stream_count_words = stream_remove_punctuation.map(lambda x: count_words(x))
    stream_count_words.print()
    env.execute()

read_from_kafka()
