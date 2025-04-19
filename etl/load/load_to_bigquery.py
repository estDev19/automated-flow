import os
import logging
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()  # Cargar variables de entorno del archivo .env

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
SALES_TABLE = "sales_table"
COMPARISON_TABLE = "sales_vs_forecast_2025"

client = bigquery.Client()

def load_dataframe_to_bq(df, table_name):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        logging.info(f"Cargando {len(df)} filas a BigQuery: {table_id}")
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()  # Esperar a que el job termine
        logging.info(f"Datos cargados exitosamente en {table_id}.")
    except Exception as e:
        logging.error(f"Error al cargar datos a BigQuery: {e}")
        raise

def load_sales_data(df):
    load_dataframe_to_bq(df, SALES_TABLE)

def load_comparison_data(df):
    load_dataframe_to_bq(df, COMPARISON_TABLE)