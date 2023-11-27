#以下代码使用kafka-python模块将数据发送到本地Kafka集群。
#此代码打开一个名为“hamlet.txt”的文本文件，并将其内容作为流发送到指定的 Kafka 主题“hamlet”：

from kafka import KafkaProducer
import time
import os

# 定义一个函数，用于将文件发送到Kafka，参数为文件路径、主题和Kafka服务器地址
def send_file_to_kafka(file_path: str, topic: str, bootstrap_servers: str):
    # 创建一个KafkaProducer实例，用于发送消息
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    # 获取文件大小
    file_size = os.path.getsize(file_path)
    # 循环发送文件
    while True:
        # 打开文件，以二进制方式读取
        with open(file_path, "rb") as f:
            # 循环读取文件
            while True:
                # 读取1024字节的数据
                data = f.read(1024)
                # 如果读取完毕，则跳出循环
                if not data:
                    break
                # 将数据发送到Kafka，并打印发送的进度
                producer.send(topic, data)
                # 计算已发送的数据量
                percent_sent = (f.tell() / file_size) * 100
                bytes_sent = len(data)
                print(f"Sent {bytes_sent} bytes {topic} {percent_sent:.2f}% sent")
                # 每3秒打印一次发送进度
                time.sleep(3)
        # 询问用户是否继续发送文件
        user_input = input("Press 'c' to continue sending the file or 'q' to quit: ")
        # 如果用户输入q，则退出循环
        if user_input == "q":
            break

# 调用函数，将hamlet.txt文件发送到Kafka，主题为hamlet，Kafka服务器地址为localhost:9092
send_file_to_kafka("./hamlet.txt",  "hamlet", "localhost:9092")

# 在此代码中，send_file_to_kafka 函数接受三个参数：file_path、topic 和 bootstrap_servers。
# file_path是本地文件的路径，topic是数据要发送到的Kafka主题，bootstrap_servers是Kafka集群的地址。
# 该函数使用with语句打开文件，读取其内容，并将它们作为流数据发送到指定的Kafka主题。
# 发送过程中，打印出发送进度，并使用time.sleep方法暂停3秒来控制发送速率。