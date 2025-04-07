#!/bin/bash

# Ubuntu 24.04 Kafka安装脚本
# 使用USTC和TUNA镜像源以提高在中国大陆的下载速度

# 遇到错误即退出
set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # 无颜色

# 配置变量
KAFKA_VERSION="4.0.0"
SCALA_VERSION="2.13"
JAVA_VERSION="openjdk-11-jdk"
KAFKA_USER="kafka"
KAFKA_GROUP="kafka"
KAFKA_HOME="/opt/kafka"
KAFKA_DATA="/var/lib/kafka"
KAFKA_LOGS="/var/log/kafka"
ZK_DATA="/var/lib/zookeeper"
ZK_LOGS="/var/log/zookeeper"

# 输出带颜色的信息
info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# 替换apt源为USTC镜像
replace_apt_sources() {
    info "替换apt源为USTC镜像..."
    sudo cp /etc/apt/sources.list /etc/apt/sources.list.backup
    sudo bash -c 'cat > /etc/apt/sources.list << EOF
deb https://mirrors.ustc.edu.cn/ubuntu/ noble main restricted universe multiverse
deb https://mirrors.ustc.edu.cn/ubuntu/ noble-updates main restricted universe multiverse
deb https://mirrors.ustc.edu.cn/ubuntu/ noble-backports main restricted universe multiverse
deb https://mirrors.ustc.edu.cn/ubuntu/ noble-security main restricted universe multiverse
EOF'
}

# 更新系统并安装依赖
update_system() {
    info "更新系统并安装依赖..."
    sudo apt update && sudo apt upgrade -y
    sudo apt install -y $JAVA_VERSION curl wget net-tools tar unzip
}

# 创建Kafka用户和目录
setup_kafka_user_and_dirs() {
    info "创建Kafka用户和目录..."
    
    # 检查用户是否存在
    if ! id -u $KAFKA_USER &>/dev/null; then
        sudo groupadd $KAFKA_GROUP
        sudo useradd -m -d /home/$KAFKA_USER -s /bin/bash -g $KAFKA_GROUP $KAFKA_USER
    else
        info "用户${KAFKA_USER}已存在，跳过创建步骤"
    fi
    
    # 创建必要的目录
    sudo mkdir -p $KAFKA_HOME $KAFKA_DATA $KAFKA_LOGS $ZK_DATA $ZK_LOGS
    
    # 设置权限
    sudo chown -R $KAFKA_USER:$KAFKA_GROUP $KAFKA_HOME $KAFKA_DATA $KAFKA_LOGS $ZK_DATA $ZK_LOGS
}

# 下载并安装Kafka
download_and_install_kafka() {
    info "下载并安装Kafka ${KAFKA_VERSION}..."
    local download_success=false
    local KAFKA_ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
    
    # 尝试从USTC镜像下载
    info "尝试从USTC镜像下载..."
    if wget -c https://mirrors.ustc.edu.cn/apache/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}; then
        download_success=true
    else
        warn "USTC镜像下载失败，尝试TUNA镜像..."
        # 尝试从TUNA镜像下载
        if wget -c https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}; then
            download_success=true
        else
            warn "TUNA镜像下载失败，尝试从官方镜像下载..."
            if wget -c https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}; then
                download_success=true
            else
                error "下载失败，请检查网络连接或Kafka版本是否存在"
            fi
        fi
    fi
    
    # 解压并安装
    info "解压并安装Kafka..."
    sudo tar -xzf ${KAFKA_ARCHIVE} -C /opt/
    sudo mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/* $KAFKA_HOME/
    sudo rmdir /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}
    
    # 设置权限
    sudo chown -R $KAFKA_USER:$KAFKA_GROUP $KAFKA_HOME
    
    # 清理下载文件
    rm -f ${KAFKA_ARCHIVE}
}

# 配置Kafka和ZooKeeper
configure_kafka() {
    info "配置Kafka和ZooKeeper..."
    
    # 配置ZooKeeper
    sudo -u $KAFKA_USER bash -c "cat > $KAFKA_HOME/config/zookeeper.properties << EOF
# ZooKeeper基本配置
dataDir=$ZK_DATA
clientPort=2181
maxClientCnxns=0
admin.enableServer=false

# 基本性能配置
tickTime=2000
initLimit=10
syncLimit=5

# 自动清理配置
autopurge.snapRetainCount=3
autopurge.purgeInterval=24
EOF"

    # 配置Kafka服务器
    sudo -u $KAFKA_USER bash -c "cat > $KAFKA_HOME/config/server.properties << EOF
# Kafka基本配置
broker.id=0
listeners=PLAINTEXT://localhost:9092
advertised.listeners=PLAINTEXT://localhost:9092
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# 日志配置
log.dirs=$KAFKA_DATA
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

# 数据保留策略
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# ZooKeeper连接
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=18000

# 删除主题特性启用
delete.topic.enable=true
EOF"
}

# 创建Systemd服务
create_systemd_services() {
    info "创建Systemd服务..."
    
    # ZooKeeper服务
    sudo bash -c "cat > /etc/systemd/system/zookeeper.service << EOF
[Unit]
Description=Apache ZooKeeper Server
Documentation=http://zookeeper.apache.org
Requires=network.target
After=network.target

[Service]
Type=simple
User=$KAFKA_USER
Group=$KAFKA_GROUP
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ExecStart=$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
ExecStop=$KAFKA_HOME/bin/zookeeper-server-stop.sh
Restart=on-abnormal
StandardOutput=append:$ZK_LOGS/zookeeper.out
StandardError=append:$ZK_LOGS/zookeeper.err

[Install]
WantedBy=multi-user.target
EOF"

    # Kafka服务
    sudo bash -c "cat > /etc/systemd/system/kafka.service << EOF
[Unit]
Description=Apache Kafka Server
Documentation=http://kafka.apache.org
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=$KAFKA_USER
Group=$KAFKA_GROUP
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ExecStart=$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
ExecStop=$KAFKA_HOME/bin/kafka-server-stop.sh
Restart=on-abnormal
StandardOutput=append:$KAFKA_LOGS/kafka.out
StandardError=append:$KAFKA_LOGS/kafka.err

[Install]
WantedBy=multi-user.target
EOF"

    # 重新加载Systemd配置
    sudo systemctl daemon-reload
}

# 配置环境变量
setup_environment() {
    info "配置环境变量..."
    
    sudo bash -c "cat > /etc/profile.d/kafka.sh << EOF
export KAFKA_HOME=$KAFKA_HOME
export PATH=\$PATH:\$KAFKA_HOME/bin
EOF"
    
    source /etc/profile.d/kafka.sh
}

# 启动Kafka和ZooKeeper服务
start_services() {
    info "启动ZooKeeper和Kafka服务..."
    
    # 启动ZooKeeper
    sudo systemctl start zookeeper
    sudo systemctl enable zookeeper
    
    # 等待ZooKeeper完全启动
    info "等待ZooKeeper启动..."
    sleep 10
    
    # 启动Kafka
    sudo systemctl start kafka
    sudo systemctl enable kafka
    
    # 等待Kafka完全启动
    info "等待Kafka启动..."
    sleep 10
}

# 验证安装
verify_installation() {
    info "验证Kafka和ZooKeeper安装..."
    
    # 检查服务状态
    zk_status=$(sudo systemctl is-active zookeeper)
    kafka_status=$(sudo systemctl is-active kafka)
    
    if [ "$zk_status" = "active" ] && [ "$kafka_status" = "active" ]; then
        info "Kafka和ZooKeeper安装验证成功！"
        echo "=================================================="
        echo "ZooKeeper状态: $zk_status"
        echo "Kafka状态: $kafka_status"
        echo "ZooKeeper端口: 2181"
        echo "Kafka端口: 9092"
        echo "=================================================="
        
        # 创建测试主题
        info "创建测试主题..."
        sudo -u $KAFKA_USER $KAFKA_HOME/bin/kafka-topics.sh --create \
            --bootstrap-server localhost:9092 \
            --replication-factor 1 \
            --partitions 1 \
            --topic test
        
        # 列出主题
        info "列出可用主题..."
        sudo -u $KAFKA_USER $KAFKA_HOME/bin/kafka-topics.sh --list \
            --bootstrap-server localhost:9092
    else
        warn "Kafka和ZooKeeper安装验证失败"
        echo "ZooKeeper状态: $zk_status"
        echo "Kafka状态: $kafka_status"
        echo "请检查日志文件:"
        echo "ZooKeeper日志: $ZK_LOGS"
        echo "Kafka日志: $KAFKA_LOGS"
    fi
}

# 显示帮助信息
show_help() {
    echo "=================================================="
    echo "Kafka管理命令:"
    echo "启动ZooKeeper: sudo systemctl start zookeeper"
    echo "启动Kafka: sudo systemctl start kafka"
    echo "停止ZooKeeper: sudo systemctl stop zookeeper"
    echo "停止Kafka: sudo systemctl stop kafka"
    echo "查看ZooKeeper状态: sudo systemctl status zookeeper"
    echo "查看Kafka状态: sudo systemctl status kafka"
    echo ""
    echo "Kafka命令行工具:"
    echo "创建主题: $KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic [主题名]"
    echo "列出主题: $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"
    echo "生产消息: $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic [主题名]"
    echo "消费消息: $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic [主题名] --from-beginning"
    echo "=================================================="
}

# 主函数
main() {
    info "开始在Ubuntu 24.04上安装Kafka ${KAFKA_VERSION}..."
    replace_apt_sources
    update_system
    setup_kafka_user_and_dirs
    download_and_install_kafka
    configure_kafka
    create_systemd_services
    setup_environment
    start_services
    verify_installation
    show_help
    info "Kafka安装完成！"
}

# 执行主函数
main