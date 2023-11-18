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
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema


# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.
# 定义一个函数，用于将数据写入kafka
def write_to_kafka(env):
    # 定义类型信息，类型为INT和STRING
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    # 从集合中读取数据，类型为INT和STRING
    ds = env.from_collection(
        [(1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=type_info)

    # 定义序列化模式，类型为INT和STRING
    serialization_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(type_info) \
        .build()
    # 定义kafka生产者，类型为INT和STRING
    kafka_producer = FlinkKafkaProducer(
        topic='test_json_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    # 注意输出类型为RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute()


def read_from_kafka(env):
    # 定义反序列化模式，类型为INT和STRING
    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW([Types.INT(), Types.STRING()])) \
        .build()
    # 定义kafka消费者，类型为INT和STRING
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_json_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'}
    )
    # 从最早的记录开始读取数据
    kafka_consumer.set_start_from_earliest()

    # 将消费者添加到环境，并打印输出
    env.add_source(kafka_consumer).print()
    env.execute()


if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 获取环境
    env = StreamExecutionEnvironment.get_execution_environment()
    # 添加flink-sql-connector-kafka-1.15.0.jar包
    env.add_jars("file:///path/to/flink-sql-connector-kafka-1.15.0.jar")

    print("start writing data to kafka")
    # 调用函数，将数据写入kafka
    write_to_kafka(env)

    print("start reading data from kafka")
    # 调用函数，从kafka读取数据
    read_from_kafka(env)