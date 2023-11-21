#Following code uses kafka-python module to send data to a local Kafka cluster. 
#This code opens a text file named `building_data.csv` and sends its contents as a stream to a specified Kafka Topic `transaction`:

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


# In this code, the send_file_to_kafka function accepts three parameters: file_path, topic, and bootstrap_servers. 
# file_path is the path to the local file, topic is the Kafka topic to which the data should be sent, and bootstrap_servers is the address of the Kafka cluster. 
# The function uses a with statement to open the file, reads its contents, and sends them as streaming data to the specified Kafka topic. 
# During the sending process, it prints out the transmission progress and uses the time.sleep method to pause for 0.1 seconds to control the sending rate. 
