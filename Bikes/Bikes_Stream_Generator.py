from kafka import KafkaProducer
import time
import os
import chardet

def send_file_to_kafka(file_path: str, topic: str, bootstrap_servers: str):
    # 创建KafkaProducer实例，用于发送消息
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
# 调用函数，将文件发送到Kafka
send_file_to_kafka("./data.csv",  "bikes", "localhost:9092")