from confluent_kafka import Producer, Consumer


def transform_data_for_kafka(sql_df, key_column, value_column):
    kafka_df = sql_df.selectExpr(f"CAST({key_column} AS STRING) AS key", f"to_json(struct({value_column})) AS value")
    return kafka_df

def publish_message(producer_instance, topic_name, key, value):
    try:
        producer_instance.produce(topic_name, key=key, value=value)
        producer_instance.flush()
    except Exception as ex:
        print('Exception in publishing message:')
        print(str(ex))

def publish_to_kafka(sql_df, kafka_bootstrap_servers, topic, key_column, value_column):
    producer_config = {
        'bootstrap.servers': kafka_bootstrap_servers,
        'client.id': 'kafka-producer'
    }

    producer = Producer(producer_config)

    kafka_df = transform_data_for_kafka(sql_df, key_column, value_column)

    for row in kafka_df.collect():
        key = row.key
        value = row.value
        publish_message(producer, topic, key, value)

def create_kafka_consumer(kafka_bootstrap_servers, kafka_topic, group_id):
    conf = {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }
    return Consumer(conf)