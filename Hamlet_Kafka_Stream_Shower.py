from kafka import KafkaConsumer

# 创建一个KafkaConsumer对象，用于从Kafka主题中读取消息
consumer = KafkaConsumer(
    # 指定要读取的消息主题
    "hamlet",
    # 指定Kafka服务器的地址和端口
    bootstrap_servers=["localhost:9092"],
    # 指定消费偏移量，可以是earliest、latest或指定specific offset
    auto_offset_reset="earliest",
    # 指定是否在消费时自动提交偏移量
    enable_auto_commit=True,
    # 指定消费组名
    group_id="my-group",
    # 指定消息反序列化函数，将字节转换为字符串
    value_deserializer=lambda x: x.decode("utf-8")
)

# 循环读取Kafka消息，并打印消息长度和消息内容
for message in consumer:
    print(f"Received {len(message.value)} bytes from Kafka topic {message.topic}")
    print(f"{message.value}")


# 在上面的代码中，我们使用`KafkaConsumer`类来创建一个消费者对象。
# 我们将 `hamlet` 作为主题名称传递给构造函数。
# 我们还传递 `localhost:9092` 作为引导服务器的地址。
# 我们使用 `value_deserializer` 参数来解码从 Kafka 主题收到的消息。
# 我们使用 `for` 循环来迭代消费者对象，并使用 `print` 函数来打印消息的内容。