################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import logging
import sys

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.csv import CsvRowSerializationSchema, CsvRowDeserializationSchema


# Make sure that the Kafka cluster is started and the topic 'test_csv_topic' is
# created before executing this job.
def write_to_kafka(env):
    # 定义类型信息，类型为INT和STRING
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    # 从集合中获取数据，类型为INT和STRING
    ds = env.from_collection([
        (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=type_info)

    # 创建序列化schema，类型为INT和STRING
    serialization_schema = CsvRowSerializationSchema.Builder(type_info).build()
    # 创建kafka生产者，类型为INT和STRING，配置为localhost:9092和test_group
    kafka_producer = FlinkKafkaProducer(
        topic='test_csv_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    # 确保输出类型为RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute()


def read_from_kafka(env):
    # 定义类型信息，类型为INT和STRING
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    # 创建反序列化schema，类型为INT和STRING
    deserialization_schema = CsvRowDeserializationSchema.Builder(type_info).build()

    # 创建kafka消费者，类型为INT和STRING，配置为localhost:9092和test_group_1
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_csv_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'}
    )
    # 从最早的记录开始读取
    kafka_consumer.set_start_from_earliest()

    # 添加源，并打印输出
    env.add_source(kafka_consumer).print()
    env.execute()


if __name__ == '__main__':
    # 设置日志输出级别和格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 获取StreamExecutionEnvironment实例
    env = StreamExecutionEnvironment.get_execution_environment()    
    # 添加flink-sql-connector-kafka-3.1-SNAPSHOT.jar包
    env.add_jars(
        "file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")


    print("start writing data to kafka")
    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)