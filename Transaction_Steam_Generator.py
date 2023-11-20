#Following code uses kafka-python module to send data to a local Kafka cluster. 
#This code opens a text file named `hamlet.txt` and sends its contents as a stream to a specified Kafka Topic `hamlet`:

from kafka import KafkaProducer
import time
import os
import chardet

def send_file_to_kafka(file_path: str, topic: str, bootstrap_servers: str):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read())
        encoding = result["encoding"]
    with open(file_path, "r", encoding=encoding) as f:
        lines_total = len(f.readlines())
    lines_send = 0
    while True:
        with open(file_path, "rb") as f:
            while True:
                data = f.readlines(10)
                if not data:
                    break
                data_str = str(data)
                data_bytes = data_str.encode()
                producer.send(topic, data_bytes)
                lines_send += 10
                percent_sent = (lines_send / lines_total) * 100                
                bytes_sent = len(data_bytes)
                print(f"Sent {bytes_sent} bytes {topic} {percent_sent:.2f}% sent")
                time.sleep(3)
        user_input = input("Press 'c' to continue sending the file or 'q' to quit: ")
        if user_input == "q":
            break

send_file_to_kafka("./transaction_data_generated.csv",  "transaction", "localhost:9092")


# In this code, the send_file_to_kafka function accepts three parameters: file_path, topic, and bootstrap_servers. 
# file_path is the path to the local file, topic is the Kafka topic to which the data should be sent, and bootstrap_servers is the address of the Kafka cluster. 
# The function uses a with statement to open the file, reads its contents, and sends them as streaming data to the specified Kafka topic. 
# During the sending process, it prints out the transmission progress and uses the time.sleep method to pause for 0.1 seconds to control the sending rate. 
