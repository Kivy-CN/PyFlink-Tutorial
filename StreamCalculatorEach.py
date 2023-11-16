import os
import re
from collections import Counter
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common import SimpleStringSchema
import argparse
import logging
import sys
from pyflink.table import StreamTableEnvironment, TableEnvironment, EnvironmentSettings, TableDescriptor, Schema,\
    DataTypes, FormatDescriptor
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udf
import pandas as pd
from pyflink.table import StreamTableEnvironment
from pyflink.table import DataTypes
from io import StringIO
import json

def remove_punctuation(text):
    return re.sub(r'[^\w\s]','',text)

def count_words(text):
    words = text.split()
    # return Counter(words)
    result = dict(Counter(words))
    return(result)

def read_from_kafka():
    # Create a Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()    
    t_env = StreamTableEnvironment.create(env)

    # Add the Flink SQL Kafka connector jar file to the classpath
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")

    # Print a message to indicate that data reading from Kafka has started
    print("start reading data from kafka")

    # Create a Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='hamlet', # The topic to consume messages from
        deserialization_schema= SimpleStringSchema('UTF-8'), # The schema to deserialize messages
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} # The Kafka broker address and consumer group ID
    )

    # Start reading messages from the earliest offset
    kafka_consumer.set_start_from_earliest()

    # Add the Kafka consumer as a source to the Flink execution environment
    stream_original_text = env.add_source(kafka_consumer)

    # Remove punctuation from the text
    stream_remove_punctuation = stream_original_text.map(lambda x: remove_punctuation(x))

    # Count the words in the text
    stream_count_words = stream_remove_punctuation.map(lambda x: count_words(x))

    # Print the word counts to the console
    stream_count_words.print()
    
    print('\n\n',type(stream_count_words),'\n\n')
    
    # Start the Flink job
    env.execute()

read_from_kafka()
