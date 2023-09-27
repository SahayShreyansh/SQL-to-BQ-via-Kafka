import argparse
from sqlserver import configure_spark, read_data_from_sql_server
from Kafkaserver import publish_to_kafka

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQL to Kafka Data Pipeline")
    parser.add_argument("--jdbc_url", required=True, help="JDBC URL for SQL Server") #jdbc:sqlserver://localhost:1433;databaseName=prac;
    parser.add_argument("--user", required=True, help="SQL Server username")
    parser.add_argument("--password", required=False, default="", help="SQL Server password")
    parser.add_argument("--table_name", required=True, help="Name of the table to read from")

    parser.add_argument("--kafka_bootstrap_servers", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--kafka_topic", required=True, help="Kafka topic to write data to")
    parser.add_argument("--key_column", required=True, help="Column to use as Kafka key")
    parser.add_argument("--value_column", required=True, help="Column to use as Kafka value")

    args = parser.parse_args()

    spark = configure_spark()
    sql_df = read_data_from_sql_server(spark, args.jdbc_url, args.user, args.password, args.table_name)

    publish_to_kafka(sql_df, args.kafka_bootstrap_servers, args.kafka_topic, args.key_column, args.value_column)
