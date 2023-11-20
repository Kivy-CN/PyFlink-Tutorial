from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import SerializationSchema

class MyKeyedProcessFunction(KeyedProcessFunction):
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context', out: 'Collector'):
        out.collect(value)

env = StreamExecutionEnvironment.get_execution_environment()
# Add the Flink SQL Kafka connector jar file to the classpath
env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")

kafka_consumer = FlinkKafkaConsumer(
    topics='data',
    deserialization_schema=SerializationSchema(Types.STRING()),
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'}
)
data_stream = env.add_source(kafka_consumer)
keyed_stream = data_stream.key_by(lambda x: x[0])
keyed_stream.process(MyKeyedProcessFunction()).print()

env.execute()
