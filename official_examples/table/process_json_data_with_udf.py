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
import json
import logging
import sys

from pyflink.table import (EnvironmentSettings, TableEnvironment, DataTypes, TableDescriptor,
                           Schema)
from pyflink.table.expressions import col
from pyflink.table.udf import udf


def process_json_data_with_udf():
    # 创建一个TableEnvironment实例，并设置为流式模式
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # define the source
    # 定义源数据
    table = t_env.from_elements(
        elements=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        schema=['id', 'data'])

    # define the sink
    # 定义sink
    t_env.create_temporary_table(
        'sink',
        TableDescriptor.for_connector('print')
                       .schema(Schema.new_builder()
                               .column('id', DataTypes.BIGINT())
                               .column('data', DataTypes.STRING())
                               .build())
                       .build())

    # update json columns
    # 更新json列
    @udf(result_type=DataTypes.STRING())
    def update_tel(data):
        # 将data转换为json格式
        json_data = json.loads(data)
        # 更新tel字段
        json_data['tel'] += 1
        # 将json_data转换为json格式
        return json.dumps(json_data)

    # 选择id和更新后的data
    table = table.select(col('id'), update_tel(col('data')))

    # execute
    # 执行
    table.execute_insert('sink') \
         .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details
    # 移除.wait()，如果提交到远程集群，参考https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # 获取更多详情
    # 执行
    # table.execute_insert('sink').wait()


if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    process_json_data_with_udf()