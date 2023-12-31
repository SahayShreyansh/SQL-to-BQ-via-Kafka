# SQL to Kafka Data Pipeline

This Python-based data pipeline connects to a SQL Server database, extracts data from a specified table, transforms it, and publishes it to a Kafka topic.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Modules](#modules)
- [License](#license)

## Prerequisites

Before using this data pipeline, ensure you have the following prerequisites installed:

- [Python](https://www.python.org/downloads/)
- [Apache Spark](https://spark.apache.org/downloads.html)
- [Confluent Kafka Python](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [MSSQL JDBC Driver](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url?view=sql-server-ver15)
- Google Cloud account with BigQuery access
- Service account credentials for BigQuery

## Installation

1. Clone this repository to your local machine:

   ```bash
   git clone https://github.com/your-username/kafkatoSql.git
   cd sql-to-kafka-pipeline
2. Install the required Python packages:  
   pip install pyspark confluent-kafka
   pip install google.cloud
3. Download the MSSQL JDBC driver and place it in the appropriate directory, as specified in sqlserver.py.


## Usage

Configure the pipeline by editing the main.py file. Provide the necessary command-line arguments to specify the JDBC URL, SQL Server credentials, table name, Kafka parameters, and key/value column names.

Run the pipeline by executing main.py:
python main.py \
  --jdbc_url "jdbc:sqlserver://localhost:1433;databaseName=YourDatabase;integratedSecurity=true;" \
  --user "YourUsername" \
  --password "YourPassword" \
  --table_name "YourTableName" \
  --kafka_bootstrap_servers "localhost:9092" \
  --kafka_topic "YourKafkaTopic" \
  --key_column "YourKeyColumn" \
  --value_column "YourValueColumn"
Replace the placeholders with your specific configurations.
