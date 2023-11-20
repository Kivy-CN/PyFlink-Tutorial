import io
import csv
from datetime import datetime, timedelta
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import ProcessFunction

def read_from_kafka():
    env = StreamExecutionEnvironment.get_execution_environment()
    kafka_consumer = FlinkKafkaConsumer(
        topics='data',
        deserialization_schema=SimpleStringSchema('UTF-8'),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'}
    )
    data_stream = env.add_source(kafka_consumer)
    data_stream.map(lambda x: io.StringIO(x)) \
        .map(lambda x: csv.reader(x, delimiter='\t')) \
        .map(lambda x: (x[2], datetime.strptime(x[5], '%Y/%m/%d %H:%M'))) \
        .key_by(lambda x: x[0]) \
        .process(MyProcessFunction()) \
        .print()

    env.execute()

class MyProcessFunction(ProcessFunction):
    def __init__(self):
        self.state = None

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        if self.state is None:
            self.state = value
        else:
            if value[1] - self.state[1] < timedelta(minutes=10):
                ctx.output('output_tag', self.state[0])
                ctx.output('output_tag', value[0])
            self.state = value

if __name__ == '__main__':
    read_from_kafka()
