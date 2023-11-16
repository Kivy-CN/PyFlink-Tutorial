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

def remove_punctuation(text):
    return re.sub(r'[^\w\s]','',text)

def count_words(text):
    words = text.split()
    # return Counter(words)
    result = dict(Counter(words))
    return(result)

def read_from_kafka():
    env = StreamExecutionEnvironment.get_execution_environment()    
    t_env = StreamTableEnvironment.create(env)
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
    table = stream_count_words.to_table(t_env, name='word_count', fields=[col('word'), col('count')])
    table.window(Tumble.over(lit(90).seconds).on(col('rowtime')).alias('w')) \
         .group_by(col('w')) \
         .select(col('w').start.alias('start_time'), col('w').end.alias('end_time'), col('word'), col('count').sum().alias('count')) \
         .execute() \
         .print()

read_from_kafka()
