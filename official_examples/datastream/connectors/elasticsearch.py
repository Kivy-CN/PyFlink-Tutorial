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

from pyflink.datastream.connectors.elasticsearch import Elasticsearch6SinkBuilder, \
    Elasticsearch7SinkBuilder, FlushBackoffType, ElasticsearchEmitter

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee


def write_to_es6(env):
    # 设置elasticsearch6的连接路径
    ELASTICSEARCH_SQL_CONNECTOR_PATH = \
        'file:///path/to/flink-sql-connector-elasticsearch6-1.16.0.jar'
    # 将连接路径添加到环境变量中
    env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

    # 从集合中创建一个流
    ds = env.from_collection(
        [{'name': 'ada', 'id': '1'}, {'name': 'luna', 'id': '2'}],
        type_info=Types.MAP(Types.STRING(), Types.STRING()))

    # 创建elasticsearch6的sink
    es_sink = Elasticsearch6SinkBuilder() \
        .set_emitter(ElasticsearchEmitter.static_index('foo', 'id', 'bar')) \
        .set_hosts(['localhost:9200']) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_bulk_flush_max_actions(1) \
        .set_bulk_flush_max_size_mb(2) \
        .set_bulk_flush_interval(1000) \
        .set_bulk_flush_backoff_strategy(FlushBackoffType.CONSTANT, 3, 3000) \
        .set_connection_username('foo') \
        .set_connection_password('bar') \
        .set_connection_path_prefix('foo-bar') \
        .set_connection_request_timeout(30000) \
        .set_connection_timeout(31000) \
        .set_socket_timeout(32000) \
        .build()

    # 将流写入elasticsearch6的sink
    ds.sink_to(es_sink).name('es6 sink')

    env.execute()


def write_to_es6_dynamic_index(env):
    # 设置elasticsearch6的连接路径
    ELASTICSEARCH_SQL_CONNECTOR_PATH = \
        'file:///path/to/flink-sql-connector-elasticsearch6-1.16.0.jar'
    # 将连接路径添加到环境变量中
    env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

    # 从集合中创建一个流
    ds = env.from_collection(
        [{'name': 'ada', 'id': '1'}, {'name': 'luna', 'id': '2'}],
        type_info=Types.MAP(Types.STRING(), Types.STRING()))

    # 创建elasticsearch6的sink
    es_sink = Elasticsearch6SinkBuilder() \
        .set_emitter(ElasticsearchEmitter.dynamic_index('name', 'id', 'bar')) \
        .set_hosts(['localhost:9200']) \
        .build()

    # 将流写入elasticsearch6的sink
    ds.sink_to(es_sink).name('es6 dynamic index sink')

    env.execute()


def write_to_es7(env):
    # 设置elasticsearch7的连接路径
    ELASTICSEARCH_SQL_CONNECTOR_PATH = \
        'file:///path/to/flink-sql-connector-elasticsearch7-1.16.0.jar'
    # 将连接路径添加到环境变量中
    env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

    # 从集合中创建一个流
    ds = env.from_collection(
        [{'name': 'ada', 'id': '1'}, {'name': 'luna', 'id': '2'}],
        type_info=Types.MAP(Types.STRING(), Types.STRING()))

    # 创建elasticsearch7的sink
    es7_sink = Elasticsearch7SinkBuilder() \
        .set_emitter(ElasticsearchEmitter.static_index('foo', 'id')) \
        .set_hosts(['localhost:9200']) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .set_bulk_flush_max_actions(1) \
        .set_bulk_flush_max_size_mb(2) \
        .set_bulk_flush_interval(1000) \
        .set_bulk_flush_backoff_strategy(FlushBackoffType.CONSTANT, 3, 3000) \
        .set_connection_username('foo') \
        .set_connection_password('bar') \
        .set_connection_path_prefix('foo-bar') \
        .set_connection_request_timeout(30000) \
        .set_connection_timeout(31000) \
        .set_socket_timeout(32000) \
        .build()

    # 将流写入elasticsearch7的sink
    ds.sink_to(es7_sink).name('es7 sink')

    env.execute()


def write_to_es7_dynamic_index(env):
    # 设置elasticsearch7的连接路径
    ELASTICSEARCH_SQL_CONNECTOR_PATH = \
        'file:///path/to/flink-sql-connector-elasticsearch7-1.16.0.jar'
    # 将连接路径添加到环境变量中
    env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

    # 从集合中创建一个流
    ds = env.from_collection(
        [{'name': 'ada', 'id': '1'}, {'name': 'luna', 'id': '2'}],
        type_info=Types.MAP(Types.STRING(), Types.STRING()))

    # 创建elasticsearch7的sink
    es7_sink = Elasticsearch7SinkBuilder() \
        .set_emitter(ElasticsearchEmitter.dynamic_index('name', 'id')) \
        .set_hosts(['localhost:9200']) \
        .build()

    # 将流写入elasticsearch7的sink
    ds.sink_to(es7_sink).name('es7 dynamic index sink')

    env.execute()


if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 获取当前的执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    # 设置并行度
    env.set_parallelism(1)

    print("start writing data to elasticsearch6")
    # 写入elasticsearch6
    write_to_es6(env)
    write_to_es6_dynamic_index(env)

    print("start writing data to elasticsearch7")
    # 写入elasticsearch7
    write_to_es7(env)
    write_to_es7_dynamic_index(env)