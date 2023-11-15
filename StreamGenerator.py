#Following code uses kafka-python module to send data to a local Kafka cluster. 
#This code opens a text file named `hamlet.txt` and sends its contents as a stream to a specified Kafka Topic `hamlet`:

from kafka import KafkaProducer
import time
import os

def send_file_to_kafka(file_path: str, topic: str, bootstrap_servers: str):
    # Create a KafkaProducer object with the given bootstrap servers
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    # Get the size of the file in bytes
    file_size = os.path.getsize(file_path)
    # Open the file in read binary mode
    while True:
        with open(file_path, "rb") as f:
            # Read the file in chunks of 1024 bytes
            while True:
                data = f.read(1024)
                # If no data is read, break out of the loop
                if not data:
                    break
                # Send the data to the given topic
                producer.send(topic, data)
                # Calculate the percentage of the file sent
                percent_sent = (f.tell() / file_size) * 100
                
                # Print the number of bytes sent to the topic and thepercentage of the file sent
                bytes_sent = len(data)
                print(f"Sent {bytes_sent} bytes {topic} {percent_sent:.2f}% sent")
                # Wait for 3 seconds
                time.sleep(3)
        # Wait for user input to continue or exit
        user_input = input("Press 'c' to continue sending the file or 'q' to quit: ")
        if user_input == "q":
            break


# Call the function with the file path, topic, and bootstrap servers
send_file_to_kafka("./hamlet.txt",  "hamlet", "localhost:9092")

# In this code, the send_file_to_kafka function accepts three parameters: file_path, topic, and bootstrap_servers. 
# file_path is the path to the local file, topic is the Kafka topic to which the data should be sent, and bootstrap_servers is the address of the Kafka cluster. 
# The function uses a with statement to open the file, reads its contents, and sends them as streaming data to the specified Kafka topic. 
# During the sending process, it prints out the transmission progress and uses the time.sleep method to pause for 0.1 seconds to control the sending rate. 
