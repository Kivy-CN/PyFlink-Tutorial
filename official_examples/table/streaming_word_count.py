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
import argparse
import logging
import sys

from pyflink.table import TableEnvironment, EnvironmentSettings, TableDescriptor, Schema,\
    DataTypes, FormatDescriptor
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udf

# 定义一个函数，用于创建一个流式应用，并从随机选择的单词列表中获取单词，计算每个单词的计数，并将其写入到输出路径中
words = ["flink", "window", "timer", "event_time", "processing_time", "state",
         "connector", "pyflink", "checkpoint", "watermark", "sideoutput", "sql",
         "datastream", "broadcast", "asyncio", "catalog", "batch", "streaming"]

max_word_id = len(words) - 1


def streaming_word_count(output_path):
    # 创建一个流式应用
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # define the source
    # randomly select 5 words per second from a predefined list
    # 定义源表
    # 从随机选择的单词列表中获取单词，并以每秒5个单词的速度生成
    t_env.create_temporary_table(
        'source',
        TableDescriptor.for_connector('datagen')
                       .schema(Schema.new_builder()
                               .column('word_id', DataTypes.INT())
                               .build())
                       .option('fields.word_id.kind', 'random')
                       .option('fields.word_id.min', '0')
                       .option('fields.word_id.max', str(max_word_id))
                       .option('rows-per-second', '5')
                       .build())
    tab = t_env.from_path('source')

    # define the sink
    # 定义输出表
    if output_path is not None:
        t_env.create_temporary_table(
            'sink',
            TableDescriptor.for_connector('filesystem')
                           .schema(Schema.new_builder()
                                   .column('word', DataTypes.STRING())
                                   .column('count', DataTypes.BIGINT())
                                   .build())
                           .option('path', output_path)
                           .format(FormatDescriptor.for_format('canal-json')
                                   .build())
                           .build())
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        t_env.create_temporary_table(
            'sink',
            TableDescriptor.for_connector('print')
                           .schema(Schema.new_builder()
                                   .column('word', DataTypes.STRING())
                                   .column('count', DataTypes.BIGINT())
                                   .build())
                           .build())

    # 定义一个用户定义函数，用于将单词id转换为单词
    @udf(result_type='string')
    def id_to_word(word_id):
        return words[word_id]

    # compute word count
    # 计算单词计数
    tab.select(id_to_word(col('word_id'))).alias('word') \
       .group_by(col('word')) \
       .select(col('word'), lit(1).count) \
       .execute_insert('sink') \
       .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details
    # 移除.wait，如果提交到远程集群，参考https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # 获取更多详情


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    streaming_word_count(known_args.output)