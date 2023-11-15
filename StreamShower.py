from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "hamlet",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: x.decode("utf-8")
)

for message in consumer:
    # Print the number of bytes received from the Kafka topic
    print(f"Received {len(message.value)} bytes from Kafka topic {message.topic}")
    # Print the contents of the message
    print(f"{message.value}")


# In the above code, we use the `KafkaConsumer` class to create a consumer object. 
# We pass `hamlet` as the topic name to the constructor. 
# We also pass `localhost:9092` as the address of the bootstrap server. 
# We use the `value_deserializer` parameter to decode the messages received from the Kafka topic. 
# We use a `for` loop to iterate over the consumer object and use the `print` function to print the contents of the message. 
