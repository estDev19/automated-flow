import os
from google.cloud import storage
import pandas as pd
from io import BytesIO

BUCKET_NAME = os.getenv("BUCKET_NAME")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_KEY_PATH")

def read_excel_from_gcs(filename):
    #Lee un archivo .xlsx desde GCS y lo retorna como un DataFrame.
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    
    data = blob.download_as_bytes()
    
    # Leer el archivo de Excel desde BytesIO
    df = pd.read_excel(BytesIO(data), engine='openpyxl') 

    return df