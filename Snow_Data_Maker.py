import pandas as pd
import numpy as np
from datetime import datetime

# Define the start and end dates
start_date = datetime(2046, 11, 1)
end_date = datetime(2047, 2, 28)

# Calculate the difference between the two dates
date_diff = end_date - start_date

# The difference is returned in days, so we convert it to minutes
# (1 day = 24 hours, 1 hour = 60 minutes)
total_minutes = date_diff.days * 24 * 60

print(f"Total number of checks: {total_minutes}")

# 生成时间数据，每分钟检测一次
time_data = pd.date_range(start='2046-11-01', end='2047-02-28', periods=total_minutes)

# 生成积雪厚度数据 
# 东北和新疆的一些地域雪荷载在0.7千牛每平方米以上。
# 新疆阿尔泰山区的雪荷载在1千牛每平方米以上。

# Define the range of snow load in kg/m^2
min_snow_load = 0  # corresponds to 0.0 kN/m^2
max_snow_load = 1000  # corresponds to 1 kN/m^2

# Generate snow load data
snow_load_data = np.random.uniform(min_snow_load, max_snow_load, total_minutes)


# 将数据保存为csv文件
data = {'time': time_data, 'snow load N/m^2': snow_load_data}
df = pd.DataFrame(data)
df.to_csv('snow_load_data.csv', index=False)
