### 安装和配置不使用 Zookeeper 的 Kafka（KRaft 模式）在 Ubuntu 24.04 上

#### 1. 更新系统包列表

```bash
sudo apt update
sudo apt upgrade -y
```

#### 2. 安装 Java

Kafka 需要 Java 运行环境。安装 OpenJDK 17：

```bash
sudo apt install openjdk-17-jdk -y
```

验证安装：

```bash
java -version
# 示例输出：
# openjdk version "17.0.6" 2023-04-18
# OpenJDK Runtime Environment (build 17.0.6+10-ssm-1ubuntu2)
# OpenJDK 64-Bit Server VM (build 17.0.6+10-ssm-1ubuntu2, mixed mode, sharing)
```

#### 3. 下载并解压 Kafka

```bash
cd /opt
sudo wget https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz
sudo tar -xzf kafka_2.13-3.8.1.tgz
sudo mv kafka_2.13-3.8.1 kafka
```

#### 4. 配置 Kafka（KRaft 模式）

编辑 `server.properties` 文件：

```bash
sudo nano /opt/kafka/config/server.properties
```

添加或修改以下配置：

```properties
# 监听所有接口，包括 CONTROLLER 和 PLAINTEXT
listeners=CONTROLLER://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9092

# 广告给客户端的地址，使用服务器的实际 IP 地址
# 仅包含 PLAINTEXT 监听器，移除 CONTROLLER 监听器
advertised.listeners=PLAINTEXT://192.168.56.101:9092

# 启用 KRaft 模式
process.roles=broker,controller

# 设置节点 ID（确保唯一）
node.id=1

# 定义控制器仲裁者
controller.quorum.voters=1@localhost:9093

# 日志目录
log.dirs=/var/lib/kafka/logs

# 控制器监听器名称
controller.listener.names=CONTROLLER

# 移除 Zookeeper 连接
# zookeeper.connect=localhost:2181  # 确保这一行被注释```

**注意**: 将 `192.168.56.101` 替换为你的服务器实际 IP 地址，例如 `192.168.56.101`。

创建日志目录：

```bash
sudo mkdir -p /var/lib/kafka/logs
sudo chown -R $(whoami):$(whoami) /var/lib/kafka
```

#### 5. 初始化 Kafka 集群

在 KRaft 模式下，需要初始化集群元数据。

```bash
sudo /opt/kafka/bin/kafka-storage.sh format -t $(sudo /opt/kafka/bin/kafka-storage.sh random-uuid) -c /opt/kafka/config/server.properties
```

#### 6. 启动 Kafka

```bash
sudo /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
```

#### 7. 设置 Kafka 为系统服务（可选）

创建 Kafka 服务文件：

```bash
sudo nano /etc/systemd/system/kafka.service
```

添加以下内容：

```ini
[Unit]
Description=Apache Kafka Server (KRaft Mode)
After=network.target

[Service]
Type=simple
User=$(whoami)
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

保存并退出。

重新加载 systemd 并启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl start kafka
sudo systemctl enable kafka
```

检查服务状态：

```bash
sudo systemctl status kafka
```

#### 8. 验证 Kafka 安装

##### 创建主题

```bash
/opt/kafka/bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

##### 列出主题

```bash
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
# 应显示 "test"
```

##### 发送消息

启动生产者：

```bash
/opt/kafka/bin/kafka-console-producer.sh --topic test --bootstrap-server localhost:9092
```

输入几条消息，然后按 `Ctrl + C` 退出。

##### 接收消息

启动消费者：

```bash
/opt/kafka/bin/kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9092
```

应显示之前发送的消息。

#### 9. 配置防火墙（如果适用）

确保防火墙允许 Kafka 端口（默认 `9092`）：

```bash
sudo ufw allow 9092/tcp
sudo ufw reload
```

#### 10. 更新 Python 脚本配置

确保你的 Python 脚本中的 `bootstrap_servers` 设置为服务器的实际 IP 地址和端口 `9092`。

```python
send_file_to_kafka("./hamlet.txt", "hamlet", "192.168.56.101:9092")
```

**注意**: 将 `192.168.56.101` 替换为你的服务器实际 IP 地址。

### 说明

- **KRaft 模式**: Kafka 的 KRaft 模式不依赖 Zookeeper，简化了集群管理。确保使用 Kafka 3.3 或更高版本以获得更好的稳定性和功能支持。
  
- **配置项**:
  - `process.roles=broker,controller`: 启用 Kafka 作为 broker 和 controller。
  - `node.id=1`: 节点 ID，确保每个节点唯一。
  - `controller.quorum.voters=1@localhost:9093`: 定义控制器仲裁者。

- **系统服务**: 将 Kafka 设置为系统服务便于管理和自动启动。

- **权限**: 为了安全性，建议创建专用用户运行 Kafka，而不是使用当前用户或 `root`。

### 进一步调试

如果 Kafka 无法正常启动，请查看日志文件以获取详细错误信息：

```bash
cat /opt/kafka/logs/server.log
```

确保所有配置正确，端口未被占用，并且 Java 环境配置无误。

如有其他问题，请提供相关日志以便进一步诊断。