from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import SimpleStringSchema

class MyKeyedProcessFunction(KeyedProcessFunction):
    def process_element(self, value, ctx, out):
        # Split the input string by comma
        fields = value.split(',')
        # Extract the fourth field and convert it to an integer
        fourth_field = int(fields[3])
        # Check if the fourth field is greater than 1000
        if fourth_field > 1000:
            # Emit the input string as output
            out.collect(value)


env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
kafka_consumer = FlinkKafkaConsumer(
    topics='data', 
    deserialization_schema= SimpleStringSchema('UTF-8'), 
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'} 
)

kafka_consumer.set_start_from_earliest()
data_stream = env.add_source(kafka_consumer)
keyed_stream = data_stream.key_by(lambda x: x[0])
keyed_stream.process(MyKeyedProcessFunction()).print()

env.execute()

