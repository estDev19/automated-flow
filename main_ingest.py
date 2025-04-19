import os
from dotenv import load_dotenv
from utils_log import setup_logger

# Cargar variables de entorno
load_dotenv()

from ingest.gmail_to_gcs import (
    gmail_authenticate,       # Autenticarse en Gmail
    search_emails,            # Buscar correos según filtros
    download_attachments,     # Descargar archivos adjuntos de los correos
    upload_to_gcs             # Subir los archivos descargados a Google Cloud Storage
)

# Configurar el logger
logger = setup_logger()

def ingest_from_gmail():
    try:
        logger.info("Iniciando la autenticación en Gmail...")
        gmail_service = gmail_authenticate()  # Se autentica con Gmail
        logger.info("Autenticación exitosa en Gmail.")

        # Busca correos que tengan el asunto y remitente especificado
        messages = search_emails(gmail_service, subject='Probando', sender='esteban03co@gmail.com')

        if not messages:
            logger.warning("No se encontraron correos que coincidan con los filtros.")
            return

        # Por cada correo encontrado, descarga los adjuntos y los sube a GCS
        for msg in messages:
            logger.info(f"Procesando el correo con ID: {msg['id']}")
            attachments = download_attachments(gmail_service, msg['id'])
            for filepath in attachments:
                try:
                    logger.info(f"Subiendo archivo {filepath} a GCS...")
                    upload_to_gcs(filepath)
                    os.remove(filepath)  # Borra el archivo local después de subirlo
                    logger.info(f"Archivo {filepath} subido y eliminado localmente.")
                except Exception as e:
                    logger.error(f"Error al subir el archivo {filepath} a GCS: {str(e)}")

        logger.info("Ingesta finalizada correctamente.")

    except Exception as e:
        logger.error(f"Error en el proceso de ingesta: {str(e)}")


if __name__ == '__main__':
    ingest_from_gmail()
