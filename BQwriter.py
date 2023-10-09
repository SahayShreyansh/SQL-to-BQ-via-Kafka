from google.cloud import bigquery
from google.oauth2 import service_account
import json

credentials_path ="C://Users//Admin//Downloads//sql-scavenger-hunt-36fe6d36061b.json"
def create_bigquery_client(credentials_path):
    credentials = service_account.Credentials.from_service_account_file(credentials_path
        , scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(credentials=credentials)


def write_to_bigquery(client, dataset_id, table_id, records):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    table = client.get_table(table_ref)

    errors = client.insert_rows_json(table, records)
    if errors:
        print("Errors while inserting rows into BigQuery:")
        for error in errors:
            print(error)


