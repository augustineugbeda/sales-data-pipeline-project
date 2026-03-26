import functions_framework
from google.cloud import bigquery
import json

# Configuration - Change these ONLY if your names are different
BQ_DATASET = 'sales'
BQ_TABLE = 'orders'
BUCKET_NAME = 'ost_sales_data'

@functions_framework.cloud_event
def load_gcs_to_bigquery(cloud_event):
    """Triggered by a new file upload to GCS. Loads the CSV into BigQuery."""
    
    try:
        data = cloud_event.data
        bucket = data["bucket"]
        filename = data["name"]
        
        print(f"File uploaded: gs://{bucket}/{filename}")
        
        if bucket != BUCKET_NAME:
            print(f"Ignoring file from bucket: {bucket}")
            return
        
        if not filename.lower().endswith('.csv'):
            print(f"Skipping non-CSV file: {filename}")
            return

        load_csv_to_bigquery(filename)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise


def load_csv_to_bigquery(filename):
    client = bigquery.Client()
    table_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
    uri = f"gs://{BUCKET_NAME}/{filename}"
    
    print(f"Starting load from: {uri}")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )
    
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    print("Waiting for load job to finish...")
    load_job.result(timeout=300)
    
    if load_job.errors:
        print("Load completed with errors:", load_job.errors)
    else:
        print(f"✅ SUCCESS! Loaded {load_job.output_rows:,} rows into {BQ_DATASET}.{BQ_TABLE}")