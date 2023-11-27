#以下代码使用kafka-python模块将数据发送到本地Kafka集群。
#此代码打开一个名为“building_data.csv”的文本文件，并将其内容作为流发送到指定的 Kafka 主题“building”：
from kafka import KafkaProducer
import time
import os
import chardet

# 定义一个函数，用于将文件发送到Kafka，参数为文件路径、主题和Kafka服务器地址
def send_file_to_kafka(file_path: str, topic: str, bootstrap_servers: str):
    # 创建一个KafkaProducer对象，用于发送消息
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    # 获取文件大小
    file_size = os.path.getsize(file_path)
    # 检测文件编码
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read())
        encoding = result["encoding"]
    # 读取文件内容
    with open(file_path, "r", encoding=encoding) as f:
        lines_total = len(f.readlines())
    lines_send = 0
    # 循环发送文件内容
    while True:
        with open(file_path, "rb") as f:
            while True:
                data = f.readlines(10)
                if not data:
                    break
                data_str = str(data)
                data_bytes = data_str.encode()
                # 发送消息
                producer.send(topic, data_bytes)
                lines_send += 10
                # 计算已发送的百分比
                percent_sent = (lines_send / lines_total) * 100                
                bytes_sent = len(data_bytes)
                print(f"Sent {bytes_sent} bytes {topic} {percent_sent:.2f}% sent")
                # 每3秒检查一次
                time.sleep(3)
        # 询问是否继续发送
        user_input = input("Press 'c' to continue sending the file or 'q' to quit: ")
        if user_input == "q":
            break

# 调用函数，将文件发送到Kafka，主题为building，服务器地址为localhost:9092
send_file_to_kafka("./building_data.csv",  "building", "localhost:9092")

# 在此代码中，send_file_to_kafka 函数接受三个参数：file_path、topic 和 bootstrap_servers。
# file_path是本地文件的路径，topic是数据要发送到的Kafka主题，bootstrap_servers是Kafka集群的地址。
# 该函数使用with语句打开文件，读取其内容，并将它们作为流数据发送到指定的Kafka主题。
# 发送过程中，打印出发送进度，并使用time.sleep方法暂停0.1秒来控制发送速率。