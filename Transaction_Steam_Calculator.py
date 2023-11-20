import csv
import io

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats import CsvRowDeserializationSchema
from pyflink.table import StreamTableEnvironment

def read_from_kafka():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///home/hadoop/Desktop/PyFlink-Tutorial/flink-sql-connector-kafka-3.1-SNAPSHOT.jar")
    print("start reading data from kafka")
    kafka_consumer = FlinkKafkaConsumer(
        topics='transaction',
        deserialization_schema= CsvRowDeserializationSchema.builder()
            .set_field_delimiter(',')
            .set_quote_character(None)
            .set_allow_comments(False)
            .set_ignore_parse_errors(False)
            .set_null_literal('NULL')
            .set_field_types(Types.ROW([
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING()
            ]))
            .build(),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my-group'}
    )

    kafka_consumer.set_start_from_earliest()
    stream = env.add_source(kafka_consumer)

    # Parse CSV data
    parsed_stream = stream.map(lambda x: [x[i].strip() for i in range(len(x))])

    # Write CSV data to file
    parsed_stream.add_sink(FileSink
        .for_row_format('/tmp/output', CsvRowSerializationSchema.builder()
            .set_field_delimiter(',')
            .set_line_delimiter('\n')
            .set_quote_character(None)
            .set_escape_character(None)
            .set_null_literal('NULL')
            .set_field_types(Types.ROW([
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING()
            ]))
            .build())
        .with_output_file_config(OutputFileConfig
            .builder()
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .with_output_file_prefix('output')
            .with_output_file_suffix('.csv')
            .build())
        .build())

    env.execute()

if __name__ == '__main__':
    read_from_kafka()
