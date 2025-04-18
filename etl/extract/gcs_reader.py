import os
import logging
from google.cloud import storage
import pandas as pd
from io import BytesIO

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BUCKET_NAME = os.getenv("BUCKET_NAME")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_KEY_PATH")

def read_excel_from_gcs(filename):
    """
    Lee un archivo .xlsx desde GCS y lo retorna como un DataFrame.
    Si ocurre algún error, se captura y se loguea.
    """
    try:
        logger.info(f"Iniciando la carga del archivo '{filename}' desde GCS...")

        # Crear cliente de GCS
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)

        # Verificar si el archivo existe
        if not blob.exists():
            logger.error(f"El archivo '{filename}' no existe en el bucket '{BUCKET_NAME}'.")
            raise FileNotFoundError(f"El archivo '{filename}' no fue encontrado en el bucket '{BUCKET_NAME}'.")

        # Descargar archivo en bytes
        data = blob.download_as_bytes()

        # Leer el archivo de Excel desde BytesIO
        df = pd.read_excel(BytesIO(data), engine='openpyxl')

        logger.info(f"Archivo '{filename}' cargado exitosamente desde GCS.")
        return df

    except FileNotFoundError as fnf_error:
        logger.error(fnf_error)
        raise

    except Exception as e:
        logger.error(f"Error al cargar el archivo '{filename}' desde GCS: {str(e)}")
        raise