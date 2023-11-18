import csv
from faker import Faker
import random
import datetime
import pandas as pd

fake = Faker()

# 生成账号和姓名
def generate_account_and_name():
    # 生成一个visa类型的信用卡号
    credit_card_number = fake.credit_card_number(card_type= 'visa')[:16]
    # 生成一个名字
    name = fake.name()
    # 生成一个18到80岁的生日
    birthdate = fake.date_of_birth(minimum_age=18, maximum_age=80)
    # 生成一个SSN号
    id_number = birthdate.strftime("%y%m%d") + str(fake.random_number(digits=3))[:9]
    # 返回信用卡号，名字，SSN号
    return (credit_card_number, name, id_number)

# 生成交易金额
def generate_transaction_amount(low = -10000, high = 10000):
    # 生成一个[low, high]之间的随机数
    amount = random.randint(low, high)
    # 判断amount的正负，生成in或者out
    direction = 'in' if amount >= 0 else 'out'
    # 返回绝对值和方向
    return (abs(amount), direction)

# 生成交易时间
def generate_transaction_time():
    # 生成一个2022年12月30日
    start_date = datetime.date(2022, 12, 30)
    # 生成一个2022年12月31日
    end_date = datetime.date(2022, 12, 31)
    # 计算两个日期的差值
    time_between_dates = end_date - start_date
    # 计算两个日期的天数差
    days_between_dates = time_between_dates.days
    # 随机生成一个[0, days_between_dates]之间的数
    random_number_of_days = random.randrange(days_between_dates)
    # 生成一个[start_date, end_date]之间的随机日期
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    # 生成一个[0, 23], [0, 59], [0, 59], [0, 999999]之间的随机时间
    # random_time = datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59), random.randint(0, 999999))
    # 生成一个[0, 23], [0, 59], [0, 59]之间的随机时间
    random_time = datetime.time(random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
    # 生成一个指定日期的指定时间
    return datetime.datetime.combine(random_date, random_time)

# 将数据写入csv文件
def write_to_csv(data,file_name = 'data'):
    # 生成文件名
    file_name = file_name+'.csv'
    # 以写的方式打开文件
    with open(file_name, "w", newline="") as f:
        # 创建csv写入器
        writer = csv.writer(f, delimiter=",")
        # 写入表头
        writer.writerow(["Credit Card Number", "Name", "ID Number", "Amount", "Direction", "Transaction Time"])
        # 遍历数据，写入csv文件
        for item in data:
            writer.writerow([item[0], item[1], item[2], item[3], item[4], item[5]])

# 生成数据和账户
# 参数：num_of_accounts：账户数量；number_of_transactions：交易次数
# 返回：transaction_list：交易列表
def generate_data_and_account(num_of_accounts, number_of_transactions):
    # 创建字典和列表
    data_dict = {}
    data_list = []
    
    # 循环生成账户和名称
    for i in range(num_of_accounts):
        credit_card_number, name, id_number = generate_account_and_name()
        # 如果id_number在字典中，则从字典中取出credit_card_number，否则将credit_card_number添加到字典中
        if id_number in data_dict:
            credit_card_number = data_dict[id_number]
        else:
            data_dict[id_number] = credit_card_number
        # 将账户信息添加到列表中
        data_list.append((credit_card_number, name, id_number))
    
    # 创建交易列表
    transaction_list = []
    # 循环生成交易信息
    for i in range(number_of_transactions):
        # 从列表中随机选择账户信息
        account = random.choice(data_list)
        # 将账户信息、交易金额、交易时间添加到交易列表中
        transaction_list.append((account[0], account[1], account[2], *generate_transaction_amount(), generate_transaction_time())) 
    # 对交易列表按照交易时间排序
    transaction_list.sort(key = lambda x: x[5])
    # 返回交易列表
    return transaction_list

num_of_accounts = 3000 # 修改这里设置你想要生成的账户数目
number_of_transactions = 100000 # 修改这里设置你想要生成的交易条目
data_list = generate_data_and_account(num_of_accounts, number_of_transactions) # 调用函数来生成账户和交易数据
# write_to_csv(data_list,'generated_data')

# 生成交易数据
df = pd.DataFrame(data_list, columns=['Credit Card Number', 'Name', 'ID Number', 'Amount', 'Direction', 'Transaction Time'])
df.to_csv('transaction_data_generated.csv', index=False)
print("Data generated successfully!")

# 对每个账号的每笔交易按照时间排序
df = df.sort_values(['Credit Card Number', 'Transaction Time'])
# 计算每个账号每笔交易的时间差
df['Time Delta'] = df.groupby('Credit Card Number')['Transaction Time'].diff()
# 创建一个空的DataFrame
new_df = pd.DataFrame(columns=df.columns)

# 遍历每个账号
for account in df['Credit Card Number'].unique():
    # 获取每个账号的每笔交易
    account_df = df[df['Credit Card Number'] == account]
    # 获取每笔交易时间差小于10分钟的交易
    account_df = account_df[(account_df['Time Delta'] <= pd.Timedelta(minutes=10))]
    # 如果交易笔数大于1，则将交易添加到new_df中
    if len(account_df) > 1:
        new_df = pd.concat([new_df, account_df])
        # new_df = pd.concat([new_df.dropna(how='all'), account_df.dropna(how='all')], axis=0)

# 删除时间差列
new_df = new_df.drop(columns=['Time Delta'])

# 将new_df保存到csv文件中
new_df.to_csv('transaction_data_suspecious.csv', index=False)

print("Data of suspecious accounts generated successfully!")

# 对每个账号的每笔交易按照时间排序
new_df = new_df.sort_values(['Credit Card Number', 'Transaction Time'])
# 创建一个空的DataFrame
fraud_df = pd.DataFrame(columns=new_df.columns)

# 警戒值，签一次交易低于低值，后一次交易高于高值，就判定为潜在危险账户
low_value =100
high_value = 1000

# 遍历每个账号
for account in new_df['Credit Card Number'].unique():
    # 获取每个账号的每笔交易
    account_df = new_df[new_df['Credit Card Number'] == account]
    # 对每笔交易按照时间排序
    account_df = account_df.sort_values('Transaction Time')
    # 遍历每笔交易
    for i in range(1, len(account_df)):
        # 如果前一笔交易金额小于低值，后一笔交易金额大于高值，则将这两笔交易添加到fraud_df中
        if account_df.iloc[i-1]['Amount'] < low_value and account_df.iloc[i]['Amount'] > high_value:
            fraud_df = pd.concat([fraud_df, account_df.iloc[i-1:i+1]])

# 将fraud_df保存到csv文件中
fraud_df.to_csv('transaction_data_fraud.csv', index=False)

print("Data of fraud accounts generated successfully!")