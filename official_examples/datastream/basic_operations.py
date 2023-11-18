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

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment


# 定义一个show函数，用于打印ds和env，并执行env
def show(ds, env):
    ds.print()
    env.execute()


# 定义一个basic_operations函数，用于进行基本操作
def basic_operations():
    # 获取StreamExecutionEnvironment实例
    env = StreamExecutionEnvironment.get_execution_environment()
    # 设置并行度为1
    env.set_parallelism(1)

    # define the source
    # 定义源数据
    ds = env.from_collection(
        collection=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        type_info=Types.ROW_NAMED(["id", "info"], [Types.INT(), Types.STRING()])
    )

    # map
    # 定义map函数，用于更新tel字段
    def update_tel(data):
        # parse the json
        # 解析json
        json_data = json.loads(data.info)
        # 更新tel字段
        json_data['tel'] += 1
        # 返回id和json字符串
        return data.id, json.dumps(json_data)

    # 调用show函数，打印ds和env，并执行env
    show(ds.map(update_tel), env)
    # (1, '{"name": "Flink", "tel": 124, "addr": {"country": "Germany", "city": "Berlin"}}')
    # (2, '{"name": "hello", "tel": 136, "addr": {"country": "China", "city": "Shanghai"}}')
    # (3, '{"name": "world", "tel": 125, "addr": {"country": "USA", "city": "NewYork"}}')
    # (4, '{"name": "PyFlink", "tel": 33, "addr": {"country": "China", "city": "Hangzhou"}}')

    # filter
    # 调用filter函数，过滤出id为1的数据，并调用map函数更新tel字段
    show(ds.filter(lambda data: data.id == 1).map(update_tel), env)
    # (1, '{"name": "Flink", "tel": 124, "addr": {"country": "Germany", "city": "Berlin"}}')

    # key by
    # 调用key_by函数，以json中的country字段为key，对数据进行分组，并调用sum函数计算tel字段的和
    show(ds.map(lambda data: (json.loads(data.info)['addr']['country'],
                              json.loads(data.info)['tel']))
           .key_by(lambda data: data[0]).sum(1), env)
    # ('Germany', 123)
    # ('China', 135)
    # ('USA', 124)
    # ('China', 167)


# 定义一个main函数，用于调用basic_operations函数
if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 调用basic_operations函数
    basic_operations()