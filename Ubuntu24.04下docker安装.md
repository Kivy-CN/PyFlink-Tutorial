# Ubuntu 24.04服务器安装Docker和Kafka并使用pykafka和pyflink测试的脚本

下面是一个完整的脚本，使用中国大陆镜像在Ubuntu Server 24.04上安装Docker、运行最新版Kafka并进行测试：

## 1. 主安装脚本

创建`kafka_setup.sh`文件：

````bash
#!/bin/bash
set -e

echo "===== 更新系统并安装依赖 ====="
# 更换apt源为阿里云
sudo sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list
sudo sed -i 's/security.ubuntu.com/mirrors.aliyun.com/g' /etc/apt/sources.list
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release

echo "===== 安装Docker ====="
# 卸载旧版本(如果存在)
sudo apt-get remove -y docker docker-engine docker.io containerd runc || true

# 添加Docker官方GPG密钥(通过阿里云镜像)
curl -fsSL https://mirrors.aliyun.com/docker-ce/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# 添加Docker仓库(通过阿里云镜像)
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://mirrors.aliyun.com/docker-ce/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 更新包索引并安装Docker
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 配置Docker守护进程使用更可靠的中国镜像
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json <<EOF
{
    "registry-mirrors": [
        "https://docker.1ms.run",
        "https://docker.xuanyuan.me"
    ]
}
EOF

# 使用1ms.run助手配置镜像
echo "===== 配置1ms.run镜像加速 ====="
curl -s https://static.1ms.run/1ms-helper/scripts/install.sh | bash /dev/stdin config:mirror

# 启动Docker并设置开机启动
sudo systemctl enable docker
sudo systemctl restart docker

# 将当前用户添加到docker组
sudo usermod -aG docker $USER
echo "需要重新登录以应用docker组权限"

echo "===== 安装Docker Compose ====="
sudo curl -L "https://mirror.ghproxy.com/https://github.com/docker/compose/releases/download/v2.25.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

echo "===== 创建Kafka配置文件 ====="
mkdir -p kafka-docker
cat > kafka-docker/docker-compose.yml <<EOF
version: '3'
services:
  zookeeper:
    image: bitnami/zookeeper:latest
    ports:
      - "2181:2181"
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    networks:
      - kafka-net

  kafka:
    image: bitnami/kafka:latest
    ports:
      - "9092:9092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
    depends_on:
      - zookeeper
    networks:
      - kafka-net

networks:
  kafka-net:
    driver: bridge
EOF

echo "===== 启动Kafka ====="
cd kafka-docker

# 尝试使用docker-compose命令，如果失败则使用docker compose插件
if command -v docker-compose &> /dev/null; then
    sudo docker-compose up -d
else
    sudo docker compose up -d
fi

echo "===== 安装Java JDK ====="
# Java是PyFlink必需的
sudo apt-get install -y openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc
source ~/.bashrc

# 验证Java安装
java -version

echo "===== 安装Python环境和依赖 ====="
# 使用清华源安装pip和相关包
sudo apt-get install -y python3-pip python3-venv
python3 -m venv kafka-test-env
source kafka-test-env/bin/activate
pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

pip install pykafka
pip install apache-flink==1.17.0

echo "===== 创建测试脚本 ====="
cat > test_kafka_pykafka.py <<EOF
#!/usr/bin/env python3
from pykafka import KafkaClient
import time

def test_kafka():
    # 等待Kafka完全启动
    print("等待Kafka启动...")
    time.sleep(30)
    
    # 连接到Kafka
    client = KafkaClient(hosts="localhost:9092")
    print("可用的主题:", client.topics)
    
    # 创建主题
    topic_name = "test-topic"
    if topic_name.encode() not in client.topics:
        print(f"主题 {topic_name} 不存在，等待自动创建...")
        time.sleep(10)
        client = KafkaClient(hosts="localhost:9092")
    
    topic = client.topics[topic_name.encode()]
    
    # 创建生产者
    producer = topic.get_sync_producer()
    
    # 生产一些消息
    for i in range(10):
        message = f"测试消息 {i}".encode()
        producer.produce(message)
        print(f"已生产: {message.decode()}")
    
    # 创建消费者
    consumer = topic.get_simple_consumer(consumer_group=b"test-group", auto_commit_enable=True,
                                         reset_offset_on_start=True)
    
    # 消费一些消息
    print("消费消息:")
    count = 0
    for message in consumer:
        if message is not None:
            print(f"已消费: {message.value.decode()}")
            count += 1
            if count >= 10:
                break
    
    print("PyKafka测试成功完成!")

if __name__ == "__main__":
    test_kafka()
EOF

cat > test_kafka_pyflink.py <<EOF
#!/usr/bin/env python3
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import time

def test_flink_kafka():
    # 创建Flink流执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    
    # 等待Kafka准备就绪
    print("等待Kafka就绪...")
    time.sleep(10)
    
    # 创建Kafka源表
    create_kafka_source = """
    CREATE TABLE kafka_source (
        message STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'test-topic',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'flink-test-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'raw'
    )
    """
    
    # 创建打印接收表
    create_print_sink = """
    CREATE TABLE print_sink (
        message STRING
    ) WITH (
        'connector' = 'print'
    )
    """
    
    # 执行SQL查询
    t_env.execute_sql(create_kafka_source)
    t_env.execute_sql(create_print_sink)
    t_env.execute_sql("""
        INSERT INTO print_sink
        SELECT message FROM kafka_source
    """)
    
    print("PyFlink Kafka测试启动成功!")

if __name__ == "__main__":
    test_flink_kafka()
EOF

echo "===== 运行测试 ====="
echo "正在运行PyKafka测试..."
python3 test_kafka_pykafka.py

echo "正在运行PyFlink测试..."
python3 test_kafka_pyflink.py

echo "===== 安装和测试完成 ====="
echo "Kafka已成功安装并通过PyKafka和PyFlink测试"
````

## 2. 执行安装脚本

运行以下命令以执行安装脚本：

```bash
chmod +x kafka_setup.sh
sudo ./kafka_setup.sh
```

## 3. 脚本说明

1. **Docker安装**：使用阿里云镜像源安装Docker和Docker Compose
2. **Kafka安装**：使用Bitnami的最新Kafka和Zookeeper镜像
3. **Python环境**：使用清华镜像源安装pykafka和apache-flink
4. **测试脚本**：
   - `test_kafka_pykafka.py`：使用pykafka连接Kafka并进行生产者/消费者测试
   - `test_kafka_pyflink.py`：使用PyFlink的表API连接Kafka并执行流处理

脚本运行完成后，您将拥有一个功能完整的Kafka环境，并通过PyKafka和PyFlink成功测试过。

注意：您可能需要重新登录以使docker组权限生效。

Similar code found with 1 license type