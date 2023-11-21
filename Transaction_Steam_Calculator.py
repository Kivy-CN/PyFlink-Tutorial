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


# 定义一个MyTimestampAssigner类，继承自TimestampAssigner类，用于提取时间戳
class MyTimestampAssigner(TimestampAssigner):    
    def extract_timestamp(self, value, record_timestamp) -> int:
    # 重写extract_timestamp方法，用于提取时间戳
        if value is None:
            logging.error("Value is None")
            return 0
        else:
            return int(value[1])



# 定义一个CountWindowProcessFunction类，继承自ProcessWindowFunction[tuple, tuple, str, TimeWindow]类，用于计算窗口内的元素数量
class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    # 重写process方法，用于计算窗口内的元素数量
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, context.window().start, context.window().end, len([e for e in elements]))]

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
    # # col_target = transpose_data[3]
    # col_target = [row[3] for row in data] 
    # print(f"column target is: {col_target[0]} ",f" typeis: {type(col_target[0])}")
    # print(f"data[0] type is {type(data[0])}",f"data[0][3] type is {type(data[0][3])}",f"data[0] len is {len(data[0])}")
    return data

def parse_tuple(x):
    try:
        return (str(x[0][0]), str(x[0][1]), int(x[0][2]), int(x[0][3]), str(x[0][4]), str(x[0][5]))
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
        topics='transaction',
        deserialization_schema= SimpleStringSchema('UTF-8'), 
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
    )
    kafka_consumer.set_start_from_earliest()
    stream = env.add_source(kafka_consumer)
    parsed_stream = stream.map(parse_csv)

    # define the watermark strategy
    # 定义时间戳策略，并设置时间戳提取器
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    # 定义窗口，并设置窗口处理函数
    data_stream = parsed_stream.map(parse_tuple)
    
    ds = data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[0], key_type=Types.STRING()) \
        .window(SlidingEventTimeWindows.of(Time.milliseconds(5), Time.milliseconds(2))) \
        .process(CountWindowProcessFunction())

    type_info = ds.get_type()
    print(type_info)
    
    # define the sink
    # 定义输出流
    if output_path is not None:
        ds.sink_to(
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
        ds.print()

    env.execute()

if __name__ == '__main__':
    read_from_kafka()
