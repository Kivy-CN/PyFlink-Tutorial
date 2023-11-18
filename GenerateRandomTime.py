import random
import datetime
import csv

# 定义开始日期和结束日期
start_date = datetime.date(2023, 11, 18)
end_date = datetime.date(2024, 11, 18)

# 定义开始时间和结束时间
start_time = datetime.time(0, 0, 0)
end_time = datetime.time(23, 59, 59)

# 定义一个空列表，用于存放随机时间
time_list = []
# 使用for循环，生成10000个随机时间
for i in range(10000):
    # 计算随机日期
    random_date = start_date + (end_date - start_date) * random.random()
    # 计算随机时间
    random_time = datetime.datetime.combine(random_date, start_time) + datetime.timedelta(seconds=random.randint(0, 86399))
    # 将随机时间添加到列表中
    time_list.append(random_time.strftime("%Y-%m-%d %H:%M:%S"))

# 使用with语句，以写入模式打开文件time_data.csv，并使用csv.writer()函数创建一个writer对象
with open('time_data.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # 写入第一行标题
    writer.writerow(['Time'])
    # 使用for循环，将随机时间写入文件中
    for time in time_list:
        writer.writerow([time])

# 打印提示信息
print("CSV file 'time_data.csv' has been created successfully!")