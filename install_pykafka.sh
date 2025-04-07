#!/bin/bash
# filepath: c:\Users\HP\Documents\GitHub\Hadoop-Tutorial\install_pykafka.sh

set -e
echo "开始安装 PyKafka 及其依赖..."

# 更新apt源为国内镜像
echo "更新apt源为清华大学镜像..."
sudo cp /etc/apt/sources.list /etc/apt/sources.list.backup
sudo bash -c 'cat > /etc/apt/sources.list << EOF
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ noble main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ noble-updates main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ noble-backports main restricted universe multiverse
deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ noble-security main restricted universe multiverse
EOF'

# 更新软件包列表
sudo apt update

# 安装基础依赖
echo "安装基础依赖..."
sudo apt install -y python3-full python3-pip python3-dev build-essential librdkafka-dev openjdk-17-jdk wget unzip

# 定义环境变量和路径
ENV_NAME="kafka_env"
ENV_PATH="$HOME/$ENV_NAME"
ACTIVATE_CMD=""
PYTHON_CMD=""
PIP_CMD=""

# 检查是否有conda环境
if command -v conda &> /dev/null; then
    echo "发现Conda环境，使用Conda创建虚拟环境..."
    
    # 配置conda使用清华镜像
    conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/
    conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main/
    conda config --set show_channel_urls yes
    
    # 创建conda环境
    conda create -y -n $ENV_NAME python=3.10
    
    # 设置激活命令
    ACTIVATE_CMD="conda activate $ENV_NAME"
    PYTHON_CMD="python"
    PIP_CMD="pip"
    
    # 激活环境并安装依赖
    echo "安装pykafka及其依赖..."
    eval "$(conda shell.bash hook)"
    conda activate $ENV_NAME
else
    echo "未发现Conda环境，使用Python venv创建虚拟环境..."
    
    # 创建Python虚拟环境
    python3 -m venv $ENV_PATH
    
    # 设置激活命令
    ACTIVATE_CMD="source $ENV_PATH/bin/activate"
    PYTHON_CMD="$ENV_PATH/bin/python"
    PIP_CMD="$ENV_PATH/bin/pip"
    
    # 激活环境
    source $ENV_PATH/bin/activate
fi

# 配置pip使用国内镜像
echo "配置pip使用清华大学镜像..."
mkdir -p ~/.pip
cat > ~/.pip/pip.conf << EOF
[global]
index-url = https://pypi.tuna.tsinghua.edu.cn/simple
trusted-host = pypi.tuna.tsinghua.edu.cn
EOF

# 安装pykafka及其依赖
echo "安装pykafka及其依赖..."
$PIP_CMD install --upgrade pip
$PIP_CMD install pykafka
$PIP_CMD install kafka-python confluent-kafka

# 安装并配置Kafka
echo "下载并安装Kafka..."
KAFKA_VERSION="4.0.0"
KAFKA_FILE="kafka_2.13-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/${KAFKA_VERSION}/${KAFKA_FILE}"
KAFKA_DIR="kafka_2.13-${KAFKA_VERSION}"

# 检查Kafka是否已经安装
if [ -d "/opt/kafka" ]; then
    echo "检测到Kafka已安装在/opt/kafka目录"
    read -p "是否重新安装Kafka? (y/n): " reinstall
    if [[ $reinstall != "y" && $reinstall != "Y" ]]; then
        echo "跳过Kafka安装..."
    else
        # 下载前清理
        echo "清理旧文件..."
        rm -rf "${KAFKA_DIR}" 2>/dev/null || true
        
        # 检查是否已下载Kafka
        if [ -f "${KAFKA_FILE}" ]; then
            echo "检测到已下载Kafka压缩包，校验文件完整性..."
            # 校验文件大小，确保不是损坏的文件
            file_size=$(stat -c%s "${KAFKA_FILE}" 2>/dev/null || stat -f%z "${KAFKA_FILE}")
            if [ "$file_size" -lt 10000000 ]; then  # 文件太小可能是损坏的
                echo "文件可能已损坏，重新下载..."
                rm "${KAFKA_FILE}"
                wget $KAFKA_URL -O ${KAFKA_FILE}
            fi
        else
            echo "下载Kafka..."
            wget $KAFKA_URL -O ${KAFKA_FILE}
        fi
        
        echo "解压Kafka..."
        tar -xzf ${KAFKA_FILE}
        
        # 验证解压结果
        if [ ! -d "${KAFKA_DIR}" ]; then
            echo "解压失败，未找到 ${KAFKA_DIR} 目录"
            echo "尝试查找解压出的目录..."
            find . -type d -name "kafka*" -maxdepth 1
            exit 1
        fi
        
        echo "移动Kafka到/opt目录..."
        # 强制删除旧目录并移动新目录
        sudo rm -rf /opt/kafka
        sudo mv -f ${KAFKA_DIR} /opt/kafka
    fi
else
    # 下载前清理
    echo "清理旧文件..."
    rm -rf "${KAFKA_DIR}" 2>/dev/null || true
    
    # 检查是否已下载Kafka
    if [ -f "${KAFKA_FILE}" ]; then
        echo "检测到已下载Kafka压缩包，校验文件完整性..."
        # 校验文件大小，确保不是损坏的文件
        file_size=$(stat -c%s "${KAFKA_FILE}" 2>/dev/null || stat -f%z "${KAFKA_FILE}")
        if [ "$file_size" -lt 10000000 ]; then  # 文件太小可能是损坏的
            echo "文件可能已损坏，重新下载..."
            rm "${KAFKA_FILE}"
            wget $KAFKA_URL -O ${KAFKA_FILE}
        fi
    else
        echo "下载Kafka..."
        wget $KAFKA_URL -O ${KAFKA_FILE}
    fi
    
    echo "解压Kafka..."
    tar -xzf ${KAFKA_FILE}
    
    # 验证解压结果
    if [ ! -d "${KAFKA_DIR}" ]; then
        echo "解压失败，未找到 ${KAFKA_DIR} 目录"
        echo "尝试查找解压出的目录..."
        find . -type d -name "kafka*" -maxdepth 1
        exit 1
    fi
    
    echo "移动Kafka到/opt目录..."
    sudo mv -f ${KAFKA_DIR} /opt/kafka
fi

# 设置Kafka目录的正确权限
echo "设置Kafka文件权限..."
sudo chown -R root:root /opt/kafka
sudo chmod -R 755 /opt/kafka
sudo find /opt/kafka/bin -name "*.sh" -exec sudo chmod 755 {} \;

# 检查Kafka目录结构
echo "检查Kafka目录结构..."
ls -la /opt/kafka
ls -la /opt/kafka/bin

# 查找Kafka启动脚本 (Kafka 4.0.0使用KRaft模式，不需要Zookeeper)
echo "查找Kafka启动脚本..."
KAFKA_SCRIPT=$(find /opt/kafka -name "*kafka*start*.sh" -type f | head -1)

if [ -z "$KAFKA_SCRIPT" ]; then
    echo "错误：找不到Kafka启动脚本!"
    echo "尝试查找所有可能的脚本:"
    find /opt/kafka -name "*.sh" | sort
    exit 1
fi

echo "找到Kafka脚本: $KAFKA_SCRIPT"

# 创建软链接到标准位置
# sudo ln -sf "$KAFKA_SCRIPT" /opt/kafka/bin/kafka-server-start.sh

# 验证Java安装
echo "验证Java安装..."
java -version

# 确保JAVA_HOME环境变量设置正确
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::")
echo "JAVA_HOME设置为: $JAVA_HOME"

# 配置Kafka KRaft模式
echo "配置Kafka KRaft模式..."

# 在临时目录中创建KRaft配置文件
KRAFT_CONFIG_DIR="/opt/kafka/config/kraft"
sudo mkdir -p $KRAFT_CONFIG_DIR

# 创建KRaft配置
sudo bash -c "cat > $KRAFT_CONFIG_DIR/server.properties << EOF
# Kafka KRaft模式配置

# 基础配置
node.id=1
process.roles=broker,controller
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
controller.listener.names=CONTROLLER
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

# KRaft必要的控制器配置
controller.quorum.voters=1@localhost:9093

# 日志目录
log.dirs=/tmp/kafka-logs

# 其他配置
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
default.replication.factor=1
EOF"

# 生成集群ID
CLUSTER_ID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
echo "生成的集群ID: $CLUSTER_ID"

# 格式化存储目录
echo "格式化 Kafka 存储目录..."
sudo KAFKA_CLUSTER_ID=$CLUSTER_ID /opt/kafka/bin/kafka-storage.sh format -t $CLUSTER_ID -c $KRAFT_CONFIG_DIR/server.properties

# 创建Kafka服务单元（使用KRaft模式）
echo "创建Kafka服务单元..."
sudo bash -c 'cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org/documentation.html
Requires=network.target
After=network.target

[Service]
Type=simple
Environment="JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64"
ExecStart=/bin/bash /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/bin/bash /opt/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal
WorkingDirectory=/opt/kafka

[Install]
WantedBy=multi-user.target
EOF'

# 启动Kafka服务
echo "启动Kafka服务..."
sudo systemctl daemon-reload
sudo systemctl enable kafka.service
sudo systemctl start kafka.service
sleep 10

# 验证安装
echo "验证Kafka安装..."
if nc -z localhost 9092; then
    echo "Kafka已成功启动在端口9092"
else
    echo "Kafka启动失败，请检查日志"
    sudo systemctl status kafka.service
    exit 1
fi

echo "创建测试主题..."
/opt/kafka/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 创建简单的Python测试脚本
echo "创建Python测试脚本..."
cat > ~/test_kafka.py << EOF
from kafka import KafkaProducer, KafkaConsumer
import threading
import time

def produce_messages():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    for i in range(5):
        message = f"测试消息 {i}".encode('utf-8')
        producer.send('test-topic', message)
        print(f"已发送: {message.decode('utf-8')}")
        time.sleep(1)
    producer.close()

def consume_messages():
    consumer = KafkaConsumer('test-topic', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest',
                             group_id='test-group')
    count = 0
    for message in consumer:
        print(f"已接收: {message.value.decode('utf-8')}")
        count += 1
        if count >= 5:
            break
    consumer.close()

if __name__ == "__main__":
    print("启动Kafka测试...")
    # 先启动消费者线程
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()
    
    # 等待消费者准备就绪
    time.sleep(2)
    
    # 启动生产者
    produce_messages()
    
    # 等待消费者完成
    consumer_thread.join()
    print("测试完成!")
EOF

# 创建启动测试的脚本
cat > ~/run_kafka_test.sh << EOF
#!/bin/bash
# 激活环境并运行测试
$ACTIVATE_CMD
$PYTHON_CMD ~/test_kafka.py
EOF

chmod +x ~/run_kafka_test.sh

echo "===================================="
echo "安装完成!"
echo "运行以下命令测试Kafka:"
echo "~/run_kafka_test.sh"
if command -v conda &> /dev/null; then
    echo "或者手动运行:"
    echo "conda activate $ENV_NAME"
    echo "python ~/test_kafka.py"
else
    echo "或者手动运行:"
    echo "source $ENV_PATH/bin/activate"
    echo "python ~/test_kafka.py"
fi
echo "===================================="