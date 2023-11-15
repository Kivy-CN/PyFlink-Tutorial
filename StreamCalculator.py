import os
# Get current absolute path
current_file_path = os.path.abspath(__file__)
# Get current dir path
current_dir_path = os.path.dirname(current_file_path)
# Change into current dir path
os.chdir(current_dir_path)
output_path = current_dir_path
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

def split(line):
    yield from line.split()

def read_from_kafka():
    # Create a Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()    

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

    # Add the Kafka consumer as a source to the Flink execution environment and print the messages to the console
    env.add_source(kafka_consumer).print()

    # # Start the Flink job
    # env.execute()

    # ds = env.from_source(source=kafka_consumer, source_name= "kafka_consumer",watermark_strategy=WatermarkStrategy.for_monotonous_timestamps() )
    # # write all the data to one file
    
    # # compute word count
    # ds = ds.flat_map(split) \
    #     .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    #     .key_by(lambda i: i[0]) \
    #     .reduce(lambda i, j: (i[0], i[1] + j[1]))

    #     # define the sink
    # if output_path is not None:
    #     ds.sink_to(
    #         sink=FileSink.for_row_format(
    #             base_path=output_path,
    #             encoder=Encoder.simple_string_encoder())
    #         .with_output_file_config(
    #             OutputFileConfig.builder()
    #             .with_part_prefix("prefix")
    #             .with_part_suffix(".ext")
    #             .build())
    #         .with_rolling_policy(RollingPolicy.default_rolling_policy())
    #         .build()
    #     )
    # else:
    #     print("Printing result to stdout. Use --output to specify output path.")
    #     # ds.print()

    #     # Step 1: Create a `StreamTableEnvironment` object.
    #     t_env = StreamTableEnvironment.create(env)

    #     # Step 2: Convert the `DataStream` object to a `Table` object.
    #     table = t_env.from_data_stream(ds)

    #     # Step 3: Convert the `Table` object to a `pandas` dataframe.
    #     df = table.to_pandas()
        
    #     df.to_csv('./DataStream_API_word_count.csv', index=False)
    #     print(df)

    # submit for execution
    env.execute()

if __name__ == '__main__':
    # Set up logging
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # Call the read_from_kafka function
    read_from_kafka()
