import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCP_PROJECT="airflow-data-pipeline-448107"
GCP_BUCKET="crypto_data_manvith"
GCP_RAW_PATH="raw_data/crypto_raw_data"
GCP_transformed_data="transformed_data/crypto_transformed_data"
BIGQUERY_DATASET="crypto_db"
BIGQUERY_TB="crypto_tb"

BIGSCHEMA=[
    {"name":"id", "type":"STRING", "mode": "REQUIRED"},
    {"name":"symbol", "type":"STRING", "mode": "REQUIRED"},
    {"name":"name", "type":"STRING", "mode": "REQUIRED"},
    {"name":"current_price", "type":"FLOAT", "mode": "NULLABLE"},
    {"name":"total_volume", "type":"FLOAT", "mode": "NULLABLE"},
    {"name":"price_change_24h", "type":"FLOAT", "mode": "NULLABLE"},
    {"name":"total_supply", "type":"FLOAT", "mode": "NULLABLE"},
    {"name":"max_supply", "type":"FLOAT", "mode": "NULLABLE"},
    {"name":"last_updated", "type":"TIMESTAMP", "mode": "NULLABLE"},
    {"name":"timestamp", "type":"TIMESTAMP", "mode": "REQUIRED"}
]



def fetch_data_from_api():
    url="https://api.coingecko.com/api/v3/coins/markets"
    params={
        "vs_currency" : "usd",
        "order" : "market_cap_asc",
        "page" : 10,
        "sparkline" : False
    }
    response = requests.get(url, params)
    data=response.json()
    

    with open("crypto.json", 'w') as f:
        json.dump(data, f)

def _transform():
    with open("crypto.json", 'r') as f:
        data = json.load(f)
    transformed_data=[]
    for item in data:
        transformed_data.append({
            "id": item["id"],
            "symbol" : item["symbol"],
            "name" : item["name"],
            "current_price": item["current_price"],
            "total_volume": item["total_volume"],
            "price_change_24h": item["price_change_24h"],
            "total_supply": item["total_supply"],
            "max_supply": item["max_supply"],
            "last_updated": item["last_updated"],
            "timestamp" : datetime.utcnow().isoformat()

        })
    
    df=pd.DataFrame(transformed_data)
    df.to_csv("transformed_data.csv", index=False)




dag=DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2025, 1, 17),
    schedule_interval= timedelta(minutes=10),
    catchup=False
)

# Fetch data from api
fetch_data=PythonOperator(
    task_id="fetch_data_from_api",
    python_callable=fetch_data_from_api,
    dag=dag,

)


#Creating new bucket
CreateNewBucket = GCSCreateBucketOperator(
    task_id="CreateNewBucket",
    bucket_name=GCP_BUCKET,
    storage_class="MULTI_REGIONAL",
    location="US",
    gcp_conn_id="google_cloud_default",
    dag=dag,
)


#Loading raw data to GCP
Load_raw_data_to_GCP=LocalFilesystemToGCSOperator(
    task_id="Load_raw_data_to_GCP",
    src="crypto.json",
    dst=GCP_RAW_PATH + "{{ ts_nodash }}.json",
    bucket=GCP_BUCKET,
    gcp_conn_id="google_cloud_default",
    dag=dag,

)


#Transforming data
transform_data = PythonOperator(
    task_id = "transform_data",
    python_callable = _transform,
    dag=dag,
)



#Loading transformed data
Load_transformed_data_to_GCP=LocalFilesystemToGCSOperator(
    task_id="Load_transformed_data_to_GCP",
    src="transformed_data.csv",
    dst=GCP_transformed_data + "{{ ts_nodash }}.csv",
    bucket=GCP_BUCKET,
    gcp_conn_id="google_cloud_default",
    dag=dag,

)


create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset", 
    dataset_id=BIGQUERY_DATASET,
    gcp_conn_id="google_cloud_default",
    dag=dag,)

create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dataset_id=BIGQUERY_DATASET,
    table_id=BIGQUERY_TB,
    schema_fields=BIGSCHEMA,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)


load_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id="load_gcs_to_bigquery",
    bucket=GCP_BUCKET,
    source_objects=[GCP_transformed_data + "{{ ts_nodash }}.csv"],
    destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BIGQUERY_TB}",
    source_format="csv",
    schema_fields=BIGSCHEMA,
    write_disposition="WRITE_APPEND",
    skip_leading_rows=1,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

fetch_data >> CreateNewBucket >> Load_raw_data_to_GCP >> transform_data >> Load_transformed_data_to_GCP >> create_dataset >> create_table >> load_gcs_to_bigquery