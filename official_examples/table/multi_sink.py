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

from pyflink.table import (EnvironmentSettings, TableEnvironment, DataTypes)
from pyflink.table.udf import udf


# 定义一个多 sink 函数
def multi_sink():
    # 创建一个 TableEnvironment 对象，并设置为流式执行模式
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # 从元素中创建一个表，并设置表的 schema
    table = t_env.from_elements(
        elements=[(1, 'Hello'), (2, 'World'), (3, "Flink"), (4, "PyFlink")],
        schema=['id', 'data'])

    # 定义两个 sink 表，并设置连接器为打印
    # define the sink tables
    t_env.execute_sql("""
        CREATE TABLE first_sink (
            id BIGINT,
            data VARCHAR
        ) WITH (
            'connector' = 'print'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE second_sink (
            id BIGINT,
            data VARCHAR
        ) WITH (
            'connector' = 'print'
        )
    """)

    # 创建一个语句集
    # create a statement set
    statement_set = t_env.create_statement_set()

    # 将 id 小于等于 3 的数据发送到第一个 sink 表中
    # emit the data with id <= 3 to the "first_sink" via sql statement
    statement_set.add_insert_sql("INSERT INTO first_sink SELECT * FROM %s WHERE id <= 3" % table)

    # 将包含 "Flink" 的数据发送到第二个 sink 表中
    # emit the data which contains "Flink" to the "second_sink"
    @udf(result_type=DataTypes.BOOLEAN())
    def contains_flink(data):
        return "Flink" in data

    second_table = table.where(contains_flink(table.data))
    statement_set.add_insert("second_sink", second_table)

    # 执行语句集，并等待执行完成
    # execute the statement set
    # remove .wait if submitting to a remote cluster, refer to
    # https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/faq/#wait-for-jobs-to-finish-when-executing-jobs-in-mini-cluster
    # for more details
    statement_set.execute().wait()


# 定义一个主函数，用于打印日志
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 调用多 sink 函数
    multi_sink()