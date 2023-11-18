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

from pyflink.common.time import Instant

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (DataTypes, TableDescriptor, Schema, StreamTableEnvironment)
from pyflink.table.expressions import col, row_interval, CURRENT_ROW
from pyflink.table.window import Over


# 定义一个函数tumble_window_demo，用于演示滚动窗口操作
def tumble_window_demo():
    # 获取当前的StreamExecutionEnvironment，并设置并行度为1
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # 创建一个StreamTableEnvironment
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # define the source with watermark definition
    # 定义一个数据源，并设置watermark
    ds = env.from_collection(
        collection=[
            (Instant.of_epoch_milli(1000), 'Alice', 110.1),
            (Instant.of_epoch_milli(4000), 'Bob', 30.2),
            (Instant.of_epoch_milli(3000), 'Alice', 20.0),
            (Instant.of_epoch_milli(2000), 'Bob', 53.1),
            (Instant.of_epoch_milli(5000), 'Alice', 13.1),
            (Instant.of_epoch_milli(3000), 'Bob', 3.1),
            (Instant.of_epoch_milli(7000), 'Bob', 16.1),
            (Instant.of_epoch_milli(10000), 'Alice', 20.1)
        ],
        type_info=Types.ROW([Types.INSTANT(), Types.STRING(), Types.FLOAT()]))

    # 将数据源转换为Table，并设置watermark
    table = t_env.from_data_stream(
        ds,
        Schema.new_builder()
              .column_by_expression("ts", "CAST(f0 AS TIMESTAMP(3))")
              .column("f1", DataTypes.STRING())
              .column("f2", DataTypes.FLOAT())
              .watermark("ts", "ts - INTERVAL '3' SECOND")
              .build()
    ).alias("ts", "name", "price")

    # define the sink
    # 定义一个sink，用于输出结果
    t_env.create_temporary_table(
        'sink',
        TableDescriptor.for_connector('print')
                       .schema(Schema.new_builder()
                               .column('name', DataTypes.STRING())
                               .column('total_price', DataTypes.FLOAT())
                               .build())
                       .build())

    # define the over window operation
    # 定义一个滚动窗口操作，并计算每个名字的总价格
    table = table.over_window(
        Over.partition_by(col("name"))
            .order_by(col("ts"))
            .preceding(row_interval(2))
            .following(CURRENT_ROW)
            .alias('w')) \
        .select(col('name'), col('price').max.over(col('w')))

    # submit for execution
    # 提交执行，并等待执行完成
    table.execute_insert('sink') \
         .wait()
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details
    # 移除.wait()，如果提交到远程集群，请参考https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # 获取更多详情


if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 调用tumble_window_demo函数
    tumble_window_demo()