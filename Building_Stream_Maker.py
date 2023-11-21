import pandas as pd
import numpy as np

# 生成时间数据
time_data = pd.date_range(start='2023-01-01', end='2023-12-31', periods=10000)

# 生成沉降数据
settlement_data = np.random.rand(10000)

# 将数据保存为csv文件
data = {'time': time_data, 'settlement': settlement_data}
df = pd.DataFrame(data)
df.to_csv('building_data.csv', index=False)
