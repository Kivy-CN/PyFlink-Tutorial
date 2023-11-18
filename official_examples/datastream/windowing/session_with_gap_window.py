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

from pyflink.datastream.connectors.file_system import FileSink, RollingPolicy, OutputFileConfig

from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.datastream.window import EventTimeSessionWindows, \
    SessionWindowTimeGapExtractor, TimeWindow


# 定义一个MyTimestampAssigner类，继承自TimestampAssigner类
class MyTimestampAssigner(TimestampAssigner):
    # 定义一个extract_timestamp方法，接收value和record_timestamp两个参数，返回值为int类型
    def extract_timestamp(self, value, record_timestamp) -> int:
        # 返回value的第二个元素，即timestamp
        return int(value[1])


# 定义一个MySessionWindowTimeGapExtractor类，继承自SessionWindowTimeGapExtractor类
class MySessionWindowTimeGapExtractor(SessionWindowTimeGapExtractor):
    # 定义一个extract方法，接收element参数，返回值为int类型
    def extract(self, element: tuple) -> int:
        # 返回element的第二个元素，即timestamp
        return element[1]


# 定义一个CountWindowProcessFunction类，继承自ProcessWindowFunction[tuple, tuple, str, TimeWindow]类
class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    # 定义一个process方法，接收key、context和elements参数，返回值为Iterable[tuple]类型
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        # 返回一个元组，包含key、context.window().start、context.window().end和elements的长度
        return [(key, context.window().start, context.window().end, len([e for e in elements]))]

# 定义一个函数，用于解析参数
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
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
    # 定义源数据
    data_stream = env.from_collection([
        ('hi', 1), ('hi', 2), ('hi', 3), ('hi', 4), ('hi', 8), ('hi', 9), ('hi', 15)],
        type_info=Types.TUPLE([Types.STRING(), Types.INT()]))

    # define the watermark strategy
    # 定义水位策略
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    # 定义窗口，并处理数据
    ds = data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x[0], key_type=Types.STRING()) \
        .window(EventTimeSessionWindows.with_gap(Time.milliseconds(5))) \
        .process(CountWindowProcessFunction(),
                 Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.INT()]))

    # define the sink
    # 定义输出
    # 如果有输出路径，则将数据写入文件，否则输出到标准输出
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