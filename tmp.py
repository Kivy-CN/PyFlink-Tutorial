import json
import logging
import sys
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

# 定义show函数，用于显示数据流
def show(ds, env):
    ds.print()
    env.execute()

# 定义update_tel函数，用于更新tel字段
def update_tel(data):
    json_data = json.loads(data.info)
    json_data['tel'] += 1
    return data.id, json.dumps(json_data)

# 定义filter_by_id函数，用于过滤id字段
def filter_by_id(data):
    return data.id == 1

# 定义map_country_tel函数，用于将国家字段和tel字段映射到元组中
def map_country_tel(data):
    json_data = json.loads(data.info)
    return json_data['addr']['country'], json_data['tel']

# 定义key_by_country函数，用于将元组中的国家字段作为key
def key_by_country(data):
    return data[0]

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(
        collection=[
            (1, '{"name": "Flink", "tel": 111, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 222, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 333, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 444, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        type_info=Types.ROW_NAMED(["id", "info"], [Types.INT(), Types.STRING()])
    )
    print('\nFirst we map it: \n')
    # 调用show函数，显示数据流
    show(ds.map(update_tel), env)
    
    print('\nThen we filter it: \n')
    # 调用show函数，显示过滤后的数据流
    show(ds.filter(filter_by_id), env)

    print('\nThen we select it: \n')
    # 调用show函数，显示按照国家字段分组后的数据流
    show(ds.map(map_country_tel).key_by(key_by_country), env)