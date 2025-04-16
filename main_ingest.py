from dotenv import load_dotenv
load_dotenv()  # Carga las variables de entorno desde un archivo .env

import os

from ingest.gmail_to_gcs import (
    gmail_authenticate,       # Autenticarse en Gmail
    search_emails,            # Buscar correos según filtros
    download_attachments,     # Descargar archivos adjuntos de los correos
    upload_to_gcs             # Subir los archivos descargados a Google Cloud Storage
)

def ingest_from_gmail():
    gmail_service = gmail_authenticate()  # Se autentica con Gmail
    print("Autenticación exitosa en Gmail.")

    # Busca correos que tengan el asunto y remitente especificado
    messages = search_emails(gmail_service, subject='Probando', sender='esteban03co@gmail.com')
    
    if not messages:
        print("No se encontraron correos que coincidan con los filtros.")
        return

    # Por cada correo encontrado, descarga los adjuntos y los sube a GCS
    for msg in messages:
        attachments = download_attachments(gmail_service, msg['id'])
        for filepath in attachments:
            upload_to_gcs(filepath)
            os.remove(filepath)  # Borra el archivo local después de subirlo

    print("Ingesta finalizada correctamente.")

if __name__ == '__main__':
    ingest_from_gmail() 
