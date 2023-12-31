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

from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow


# 定义一个MyTimestampAssigner类，继承自TimestampAssigner类，用于提取时间戳
class MyTimestampAssigner(TimestampAssigner):
    # 重写extract_timestamp方法，用于提取时间戳
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[1])


# 定义一个CountWindowProcessFunction类，继承自ProcessWindowFunction[tuple, tuple, str, TimeWindow]类，用于计算窗口内的元素数量
# `CountWindowProcessFunction`类是一个窗口处理函数，用于计算窗口内的元素数量。该类接收四个参数：
# - `key`：表示窗口的键。
# - `context`：表示窗口的上下文，包括窗口的开始时间、结束时间等信息。
# - `elements`：表示窗口内的元素，是一个可迭代对象。
# - `out`：表示输出结果的`Collector`对象。
# `CountWindowProcessFunction`类的`process`方法用于计算窗口内的元素数量。
# 该方法接收三个参数：`key`、`context`和`elements`。
# 可以使用`elements`参数计算窗口内的元素数量，并将结果输出到`out`参数中。
# 输出的结果是一个元组，包含窗口的键、窗口的开始时间、窗口的结束时间以及窗口内的元素数量。
# 运算过程如下：
# 1. 将窗口内的元素保存在一个可迭代对象中。
# 2. 计算可迭代对象的长度，即窗口内的元素数量。
# 3. 将窗口的键、开始时间、结束时间和元素数量组成一个元组。
# 4. 将元组输出到`out`参数中。
# `CountWindowProcessFunction`类的`process`方法是在窗口关闭时调用的，因此在该方法中可以访问窗口内的所有元素。

class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    # 重写process方法，用于计算窗口内的元素数量
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, context.window().start, context.window().end, len([e for e in elements]))]


# 定义主函数，用于解析参数，并提交执行
if __name__ == '__main__':
    # 创建一个参数解析器，用于解析参数
    parser = argparse.ArgumentParser()
    # 添加参数，用于指定输出文件
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    # 解析参数
    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    output_path = known_args.output

    # 获取执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    # write all the data to one file
    # 设置并行度
    env.set_parallelism(1)

    # define the source
    # 定义源数据流
    data_stream = env.from_collection([
        ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 5), ('hi', 8), ('hi', 9), ('hi', 15)],
        type_info=Types.TUPLE([Types.STRING(), Types.INT()]))

    # define the watermark strategy
    # 定义时间戳策略，并设置时间戳提取器
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    # 定义窗口，并设置窗口处理函数
    ds = data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[0], key_type=Types.STRING()) \
        .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2))) \
        .process(CountWindowProcessFunction(),
                 Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.INT()]))

    # define the sink
    # 定义输出流
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