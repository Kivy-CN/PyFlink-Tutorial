import platform
import os
# # Get current absolute path
# current_file_path = os.path.abspath(__file__)
# # Get current dir path
# current_dir_path = os.path.dirname(current_file_path)
# # Change into current dir path
# os.chdir(current_dir_path)
# output_path = current_dir_path

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

def beep():
    if platform.system() == "Windows":
        import winsound
        winsound.Beep(440, 1000)
    elif platform.system() == "Linux":
        os.system("beep")
    else:
        print("Unsupported platform")

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

def count_rows(data):
    row_count = len(data)
    type_count = type(data)
    print(f"Received {row_count} rows of {type_count} data.")
    return data 

def check_data(data):
    # transpose_data = list(zip(*data))
    # col_target = transpose_data[3]
    # col_target = [row[3] for row in data] 
    # print(f"column target is: {col_target[0]} ",f" typeis: {type(col_target[0])}")
    # print(f"data[0] type is {type(data[0])}",f"data[0][3] type is {type(data[0][3])}",f"data[0] len is {len(data[0])}")
    try:
        if abs(int(data[0][1])) >= 0.5:
            beep()
            print(f"data[0][3] is {(data[0][1])}",f" ABS Larger than 0.5!\n")
    except ValueError:
        pass
    return data


def parse_tuple(x):
    try:
        return (datetime.strptime(str(x[0][0]), "%Y-%m-%d %H:%M:%S").timestamp(), float(x[0][1]))
    except ValueError:
        logging.error(f"Failed to parse tuple: {x}")
        return None

def read_from_kafka():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')
    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    output_path = known_args.output
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    kafka_consumer = FlinkKafkaConsumer(
        topics='building',
        deserialization_schema= SimpleStringSchema('UTF-8'), 
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    kafka_consumer.set_start_from_earliest()
    stream = env.add_source(kafka_consumer)
    parsed_stream = stream.map(parse_csv).map(parse_tuple)

    data_stream = parsed_stream.map(check_data)

    
    # define the sink
    # 定义输出流
    if output_path is not None:
        data_stream.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        # data_stream.print()
                
        # 调用函数，传入data_stream作为参数
        

    env.execute()

if __name__ == '__main__':
    read_from_kafka()
