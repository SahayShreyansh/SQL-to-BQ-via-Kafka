import argparse

from confluent_kafka import KafkaError

from sqlserver import configure_spark, read_data_from_sql_server
from Kafkaserver import publish_to_kafka, create_kafka_consumer
from BQwriter import create_bigquery_client, write_to_bigquery
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQL to Kafka Data Pipeline")
    parser.add_argument("--jdbc_url", required=True, help="JDBC URL for SQL Server") #jdbc:sqlserver://localhost:1433;databaseName=initial;
    parser.add_argument("--user", required=True, help="SQL Server username")
    parser.add_argument("--password", required=False, default="", help="SQL Server password")
    parser.add_argument("--table_name", required=True, help="Name of the table to read from")#start
    parser.add_argument("--kafka_bootstrap_servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--kafka_topic", required=True, help="Kafka topic to write data to")
    parser.add_argument("--key_column", required=True, help="Column to use as Kafka key")
    parser.add_argument("--value_column", required=True, help="Column to use as Kafka value")
    args = parser.parse_args()
    dataset_id = "sql-scavenger-hunt.ABC"
    table_id = "sql-scavenger-hunt.ABC.test"
    credentials_path = "C://Users//Admin//Downloads//sql-scavenger-hunt-36fe6d36061b.json"
    consumer_group_id = "my_consumer_group"
    spark = configure_spark()
    sql_df = read_data_from_sql_server(spark, args.jdbc_url, args.user, args.password, args.table_name)

    publish_to_kafka(sql_df, args.kafka_bootstrap_servers, args.kafka_topic, args.key_column, args.value_column)
    bigquery_client = create_bigquery_client(credentials_path)
    kafka_consumer = create_kafka_consumer(args.kafka_bootstrap_servers, args.kafka_topic, consumer_group_id)

    while True:
        msg = kafka_consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of partition")
            else:
                print(f"Error while polling Kafka: {msg.error()}")
        else:
            try:
                # Assuming the Kafka message value contains JSON data
                kafka_message_value = json.loads(msg.value().decode("utf-8"))

                # Process the Kafka message value here as needed

                # Write the Kafka message value to BigQuery
                write_to_bigquery(
                    bigquery_client,
                    dataset_id,
                    table_id,
                    [kafka_message_value]  # Wrap the message in a list to insert as a single row
                )

            except json.JSONDecodeError as e:
                print(f"Error decoding Kafka message: {e}")
            except Exception as e:
                print(f"Error processing Kafka message: {e}")
