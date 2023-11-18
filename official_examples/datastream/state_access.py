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
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig


# 定义一个Sum类，继承自KeyedProcessFunction
class Sum(KeyedProcessFunction):

    # 初始化函数
    def __init__(self):
        self.state = None

    # 打开函数，获取运行时上下文
    def open(self, runtime_context: RuntimeContext):
        # 创建一个状态描述符，类型为float
        state_descriptor = ValueStateDescriptor("state", Types.FLOAT())
        # 创建一个状态TTL配置，设置TTL时间为1秒，更新类型为OnReadAndWrite，禁用后台清理
        state_ttl_config = StateTtlConfig \
            .new_builder(Time.seconds(1)) \
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite) \
            .disable_cleanup_in_background() \
            .build()
        # 启用TTL，并传入TTL配置
        state_descriptor.enable_time_to_live(state_ttl_config)
        # 获取状态
        self.state = runtime_context.get_state(state_descriptor)

    # 处理元素函数
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        # retrieve the current count
        # 获取当前状态
        current = self.state.value()
        # 如果当前状态为空，则设置为0
        if current is None:
            current = 0

        # update the state's count
        # 更新状态的计数
        current += value[1]
        # 更新状态
        self.state.update(current)

        # 返回元组
        yield value[0], current


# 定义一个state_access_demo函数，用于演示状态访问
def state_access_demo():
    # 获取运行时环境
    env = StreamExecutionEnvironment.get_execution_environment()

    # 从集合中创建一个流
    ds = env.from_collection(
        collection=[
            ('Alice', 110.1),
            ('Bob', 30.2),
            ('Alice', 20.0),
            ('Bob', 53.1),
            ('Alice', 13.1),
            ('Bob', 3.1),
            ('Bob', 16.1),
            ('Alice', 20.1)
        ],
        type_info=Types.TUPLE([Types.STRING(), Types.FLOAT()]))

    # apply the process function onto a keyed stream
    # 应用处理函数，对流中的每一个元素进行处理
    ds.key_by(lambda value: value[0]) \
      .process(Sum()) \
      .print()

    # submit for execution
    # 提交执行
    env.execute()


# 调用state_access_demo函数
if __name__ == '__main__':
    state_access_demo()