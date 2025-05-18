# Ubuntu 24.04 Flink安装和Python测试脚本

下面是一个完整的脚本，用于在Ubuntu 24.04上通过本土国镜像源安装Apache Flink并使用Python进行测试：

```bash
#!/bin/bash
# 设置错误时脚本退出
set -e

echo "========== 开始安装Flink并配置Python环境 =========="

# 更新apt源为中科大源
echo "正在更新APT源为中科大镜像..."
sudo cp /etc/apt/sources.list /etc/apt/sources.list.backup
sudo tee /etc/apt/sources.list > /dev/null << EOF
deb https://mirrors.ustc.edu.cn/ubuntu/ noble main restricted universe multiverse
deb https://mirrors.ustc.edu.cn/ubuntu/ noble-updates main restricted universe multiverse
deb https://mirrors.ustc.edu.cn/ubuntu/ noble-backports main restricted universe multiverse
deb https://mirrors.ustc.edu.cn/ubuntu/ noble-security main restricted universe multiverse
EOF

# 更新包列表
sudo apt update

# 安装Java（Flink需要Java环境）
echo "正在安装Java..."
sudo apt install -y openjdk-17-jdk wget curl
echo "Java安装完成，版本："
java -version

# 创建安装目录
mkdir -p ~/flink
cd ~/flink

# 下载Flink (从阿里云镜像)
FLINK_VERSION="1.18.1"
SCALA_VERSION="2.12"
echo "正在从阿里云镜像下载Flink ${FLINK_VERSION}..."
wget https://mirrors.aliyun.com/apache/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz

# 解压Flink
echo "正在解压Flink..."
tar -xzf flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz
cd flink-${FLINK_VERSION}

# 设置环境变量
echo "配置Flink环境变量..."
echo 'export FLINK_HOME=~/flink/flink-'${FLINK_VERSION} >> ~/.bashrc
echo 'export PATH=$PATH:$FLINK_HOME/bin' >> ~/.bashrc
source ~/.bashrc

# 下载并安装Miniconda3（从清华源）
echo "下载Miniconda3..."
cd ~
MINICONDA_INSTALLER="Miniconda3-latest-Linux-x86_64.sh"
wget https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/${MINICONDA_INSTALLER}

echo "安装Miniconda3..."
bash ${MINICONDA_INSTALLER} -b -p $HOME/miniconda3
rm ${MINICONDA_INSTALLER}

# 初始化conda并配置清华源
echo "配置Conda环境..."
$HOME/miniconda3/bin/conda init bash
source ~/.bashrc
conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/
conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main/
conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud/conda-forge/
conda config --set show_channel_urls yes

# 创建Python 3.9环境
echo "创建Python 3.9环境..."
conda create -y -n pyflink_39 python=3.9
conda activate pyflink_39

# 配置pip使用清华源
echo "配置pip使用清华镜像源..."
mkdir -p ~/.pip
cat > ~/.pip/pip.conf << EOF
[global]
index-url = https://pypi.tuna.tsinghua.edu.cn/simple
trusted-host = pypi.tuna.tsinghua.edu.cn
EOF

# 安装PyFlink
echo "安装PyFlink..."
pip install apache-flink==${FLINK_VERSION}

# 创建一个简单的PyFlink测试脚本
echo "创建Flink Python测试脚本..."
cat > ~/flink_test.py << EOF
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # 创建流处理环境
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)
    
    # 打印环境信息
    print("Flink版本:", t_env.get_current_catalog())
    
    # 创建一个简单表并查询
    t_env.execute_sql("""
        CREATE TABLE random_source (
            id BIGINT,
            data STRING
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '5',
            'fields.id.kind' = 'sequence',
            'fields.id.start' = '1',
            'fields.id.end' = '10',
            'fields.data.length' = '10'
        )
    """)
    
    table = t_env.from_path("random_source")
    
    print("表结构:", table.get_schema())
    
    # 执行查询并打印结果
    result = t_env.execute_sql("SELECT * FROM random_source")
    result.print()

if __name__ == '__main__':
    main()
EOF

# 创建启动脚本，以便在正确的conda环境中运行
cat > ~/run_flink_test.sh << EOF
#!/bin/bash
eval "\$(~/miniconda3/bin/conda shell.bash hook)"
conda activate pyflink_39
python ~/flink_test.py
EOF

chmod +x ~/run_flink_test.sh

# 启动Flink集群
echo "启动Flink集群..."
cd ~/flink/flink-${FLINK_VERSION}
./bin/start-cluster.sh

echo "Flink Web UI 可通过 http://localhost:8081 访问"
echo "等待集群启动..."
sleep 10

# 运行Python测试脚本
echo "运行Python测试脚本..."
~/run_flink_test.sh

echo "========== Flink安装和测试完成 =========="
echo "如需再次运行测试，请执行: ~/run_flink_test.sh"
echo "如需停止Flink集群，请运行: ~/flink/flink-${FLINK_VERSION}/bin/stop-cluster.sh"
echo "如需激活Python环境，请运行: conda activate pyflink_39"
```



## 使用方法

1. 将上面的脚本保存为 `install_flink_cn.sh`
2. 添加执行权限：
   ```bash
   chmod +x install_flink_cn.sh
   ```
3. 执行脚本：
   ```bash
   ./install_flink_cn.sh
   ```

## 脚本说明

- 将Ubuntu软件源更改为中科大镜像
- 安装OpenJDK 17（Flink需要Java环境）
- 从阿里云镜像下载Flink 1.18.1
- 配置pip使用清华大学镜像源
- 安装PyFlink包
- 创建并运行一个简单的Python测试脚本
- 自动启动Flink集群

安装完成后，可以通过 `http://localhost:8081` 访问Flink的Web UI。

如果需要更改Flink版本，修改脚本中的`FLINK_VERSION`变量。