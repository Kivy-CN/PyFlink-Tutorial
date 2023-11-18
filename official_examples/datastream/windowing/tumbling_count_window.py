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
import sys

import argparse
from typing import Iterable

from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy

from pyflink.common import Types, Encoder
from pyflink.datastream import StreamExecutionEnvironment, WindowFunction
from pyflink.datastream.window import CountWindow


# 定义一个SumWindowFunction类，继承自WindowFunction，参数为tuple，tuple，str，CountWindow
class SumWindowFunction(WindowFunction[tuple, tuple, str, CountWindow]):
    # 定义apply函数，参数为key，window，inputs，返回值为一个列表
    def apply(self, key: str, window: CountWindow, inputs: Iterable[tuple]):
        # 定义一个变量result，初始值为0
        result = 0
        # 遍历inputs，将inputs中的第一个元素加到result中
        for i in inputs:
            result += i[0]
        # 返回一个列表，其中包含key和result
        return [(key, result)]


# 如果是主函数，则执行以下操作
if __name__ == '__main__':
    # 创建一个参数解析器，添加参数output，required=False，help='Output file to write results to.'
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    # 获取参数argv，并将其赋值给argv
    argv = sys.argv[1:]
    # 解析参数argv，将known_args赋值给known_args，将argv中剩余的参数赋值给argv
    known_args, _ = parser.parse_known_args(argv)
    output_path = known_args.output

    # 获取一个StreamExecutionEnvironment实例，并设置并行度为1
    env = StreamExecutionEnvironment.get_execution_environment()
    # 设置并行度为1
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    # 定义一个数据流，从集合中读取数据，类型为tuple，tuple，str，CountWindow
    data_stream = env.from_collection([
        (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello'), (6, 'hello')],
        type_info=Types.TUPLE([Types.INT(), Types.STRING()]))

    # 定义一个数据流，对数据流进行key_by操作，key_type为Types.STRING，并调用count_window操作，窗口大小为2，并调用apply操作，使用SumWindowFunction，返回类型为Types.TUPLE([Types.STRING(), Types.INT()])
    ds = data_stream.key_by(lambda x: x[1], key_type=Types.STRING()) \
        .count_window(2) \
        .apply(SumWindowFunction(), Types.TUPLE([Types.STRING(), Types.INT()]))

    # define the sink
    # 定义一个sink，如果output_path不为空，则将数据流ds写入到output_path指定的文件中，否则将数据流ds打印到标准输出中
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

    # submit for execution
    # 提交执行
    env.execute()