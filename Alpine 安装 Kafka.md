

```Bash
# 安装 sudo
apk update
apk add sudo

# 将当前用户添加到 wheel 组
adduser <your-username> wheel

# 配置 sudoers 文件以允许 wheel 组使用 sudo
echo '%wheel ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
# 更新包列表并安装依赖
apk update
apk add openjdk11 wget tar

# 设置 JAVA_HOME 环境变量
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$PATH:$JAVA_HOME/bin

# 下载 Kafka
wget https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz

# 解压 Kafka
tar -xzf kafka_2.13-3.8.1.tgz
cd kafka_2.13-3.8.1

# 启动 Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# 在新终端启动 Kafka 服务器
bin/kafka-server-start.sh config/server.properties
```


```Bash
# 更新包列表并安装依赖
apk update
apk add openjdk11 wget tar

# 设置 JAVA_HOME 环境变量
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$PATH:$JAVA_HOME/bin

# 下载 Flink
wget https://downloads.apache.org/flink/flink-1.20.0/flink-1.20.0-bin-scala_2.12.tgz

# 解压 Flink
tar -xzf flink-1.20.0-bin-scala_2.12.tgz
cd flink-1.20.0

# 启动 Flink 集群
./bin/start-cluster.sh

# 验证 Flink 状态
./bin/flink list
```


```Bash
# Update the package index
sudo apk update

# Install python3 and pip
sudo apk add python3 py3-pip

# Verify installation
python3 --version
pip3 --version
```

```Bash
# 安装 Xorg 和 Openbox
# 更新包列表
apk update
# Update APK repositories
sudo apk update

# Install XFCE and necessary packages
sudo apk add xfce4 xfce4-terminal dbus xfce4-session

# Install a display manager (e.g., LightDM)
sudo apk add lightdm lightdm-gtk-greeter

# Enable and start LightDM
sudo rc-update add dbus default
sudo rc-update add lightdm
sudo service dbus start
sudo service lightdm start
# 安装 Xorg
apk add xorg-server xf86-video-vesa

# 安装一个轻量级窗口管理器，例如 Openbox
apk add openbox

# 可选：安装一个简单的显示管理器，如 LightDM
apk add lightdm lightdm-gtk-greeter

# 启动并添加 LightDM 到启动项
rc-update add lightdm
service lightdm start

# 如果不使用显示管理器, 可以使用 startx
echo "exec openbox-session" > ~/.xinitrc
startx
# Install common fonts
sudo apk add ttf-dejavu

# Set locale to UTF-8
sudo apk add musl-locales musl-locales-lang
echo "export LANG=en_US.UTF-8" >> ~/.profile
source ~/.profile

# Restart LightDM
sudo service lightdm restart
```

```Bash
# 更新包列表并安装依赖
apk update
apk add openjdk11 wget tar ssh

# 设置 JAVA_HOME 环境变量
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$PATH:$JAVA_HOME/bin

# 下载 Hadoop
# wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
wget https://downloads.apache.org/hadoop/common/current/hadoop-3.4.1.tar.gz

# 解压 Hadoop
tar -xzf hadoop-3.4.1.tar.gz
mv hadoop-3.4.1 /opt/hadoop

# 设置 Hadoop 环境变量
echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# 配置 SSH 无密码登录（Hadoop 需要）
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# 配置 Hadoop 环境
cp $HADOOP_HOME/etc/hadoop/mapred-site.xml.template $HADOOP_HOME/etc/hadoop/mapred-site.xml

# 编辑配置文件（示例）
cat <<EOL > $HADOOP_HOME/etc/hadoop/core-site.xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOL

cat <<EOL > $HADOOP_HOME/etc/hadoop/hdfs-site.xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///opt/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///opt/hadoop/data/datanode</value>
    </property>
</configuration>
EOL

cat <<EOL > $HADOOP_HOME/etc/hadoop/mapred-site.xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
EOL

cat <<EOL > $HADOOP_HOME/etc/hadoop/yarn-site.xml
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>localhost:8032</value>
    </property>
</configuration>
EOL

# 创建必要的目录
mkdir -p /opt/hadoop/data/namenode
mkdir -p /opt/hadoop/data/datanode

# 格式化 Hadoop 文件系统
hdfs namenode -format

# 启动 Hadoop 服务
start-dfs.sh
start-yarn.sh

# 验证 Hadoop 状态
jps
```