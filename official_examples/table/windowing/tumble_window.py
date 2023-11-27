import logging
import sys
from pyflink.common.time import Instant
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (DataTypes, TableDescriptor, Schema, StreamTableEnvironment)
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble

def tumble_window_demo():
    # 获取当前的StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    # 设置并行度为1
    env.set_parallelism(1)
    # 创建StreamTableEnvironment
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    # 定义源，并设置watermark
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

    # 将源转换为Table
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
    # 定义sink
    t_env.create_temporary_table(
        'sink',
        TableDescriptor.for_connector('print')
                       .schema(Schema.new_builder()
                               .column('name', DataTypes.STRING())
                               .column('total_price', DataTypes.FLOAT())
                               .column('w_start', DataTypes.TIMESTAMP_LTZ())
                               .column('w_end', DataTypes.TIMESTAMP_LTZ())
                               .build())
                       .build())

    # 定义 tumble window 操作
    table = table.window(Tumble.over(lit(5).seconds).on(col("ts")).alias("w")) \
                 .group_by(col('name'), col('w')) \
                 .select(col('name'), col('price').sum, col("w").start, col("w").end)

    # 提交执行
    table.execute_insert('sink') \
         .wait()
    # 移除.wait() 如果是提交到远程集群，参考https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # 获取更多详情

if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    tumble_window_demo()