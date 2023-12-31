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

import pandas as pd
import numpy as np

from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings)


def conversion_from_dataframe():
    # 创建一个TableEnvironment对象，并设置为流式处理模式
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    # 设置并行度为1
    t_env.get_config().set("parallelism.default", "1")

    # define the source with watermark definition
    # 定义源，使用水印定义
    pdf = pd.DataFrame(np.random.rand(1000, 2))
    table = t_env.from_pandas(
        pdf,
        schema=DataTypes.ROW([DataTypes.FIELD("a", DataTypes.DOUBLE()),
                              DataTypes.FIELD("b", DataTypes.DOUBLE())]))

    # 打印出Pandas DataFrame
    print(table.to_pandas())


if __name__ == '__main__':
    # 设置日志输出格式
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    # 调用函数
    conversion_from_dataframe()