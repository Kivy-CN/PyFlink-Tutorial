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
from pyflink.datastream.formats.avro import AvroRowSerializationSchema, AvroRowDeserializationSchema


# Make sure that the Kafka cluster is started and the topic 'test_avro_topic' is
# created before executing this job.
def write_to_kafka(env):
    # 从集合中创建一个流，类型为INT和STRING
    ds = env.from_collection([
        (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))

    # 创建一个AvroRowSerializationSchema，用于序列化数据
    serialization_schema = AvroRowSerializationSchema(
        avro_schema_string="""
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }"""
    )

    # 创建一个FlinkKafkaProducer，用于将数据写入Kafka
    kafka_producer = FlinkKafkaProducer(
        topic='test_avro_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
    )

    # note that the output type of ds must be RowTypeInfo
    # 设置ds的输出类型为RowTypeInfo，以便将数据写入Kafka
    ds.add_sink(kafka_producer)
    env.execute()


def read_from_kafka(env):
    # 创建一个AvroRowDeserializationSchema，用于反序列化数据
    deserialization_schema = AvroRowDeserializationSchema(
        avro_schema_string="""
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }"""
    )

    # 创建一个FlinkKafkaConsumer，用于从Kafka读取数据
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_avro_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group_1'}
    )
    kafka_consumer.set_start_from_earliest()

    # 将从Kafka读取的数据打印出来
    env.add_source(kafka_consumer).print()
    env.execute()


if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 获取StreamExecutionEnvironment实例
    env = StreamExecutionEnvironment.get_execution_environment()
    # 添加flink-sql-avro和flink-sql-connector-kafka的jar包
    env.add_jars("file:///path/to/flink-sql-avro-1.15.0.jar",
                 "file:///path/to/flink-sql-connector-kafka-1.15.0.jar")

    print("start writing data to kafka")
    # 调用write_to_kafka函数，将数据写入Kafka
    write_to_kafka(env)

    print("start reading data from kafka")
    # 调用read_from_kafka函数，从Kafka读取数据
    read_from_kafka(env)