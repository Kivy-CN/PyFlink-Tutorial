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

from pyflink.datastream import StreamExecutionEnvironment


def process_json_data():
    # 获取执行环境
    env = StreamExecutionEnvironment.get_execution_environment()

    # define the source
    # 定义源数据
    ds = env.from_collection(
        collection=[
            (1, '{"name": "Flink", "tel": 111, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 222, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 333, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 444, "addr": {"country": "China", "city": "Hangzhou"}}')]
    )

    # 定义更新电话号码的函数
    def update_tel(data):
        # parse the json
        # 解析json数据
        json_data = json.loads(data[1])
        # 更新电话号码
        json_data['tel'] += 1
        # 返回更新后的数据
        return data[0], json_data

    # 定义过滤函数，过滤掉国家不是中国的数据
    def filter_by_country(data):
        # the json data could be accessed directly, there is no need to parse it again using
        # json.loads
        # 直接访问json数据，不需要使用json.loads
        return "China" in data[1]['addr']['country']

    # 调用map函数，更新电话号码，并过滤掉国家不是中国的数据
    ds.map(update_tel).filter(filter_by_country).print()

    # submit for execution
    # 提交执行
    env.execute()


if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 调用process_json_data函数
    process_json_data()