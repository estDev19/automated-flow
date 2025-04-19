import os
import base64
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import storage
from email import message_from_bytes

# ------------------------ CONFIGURACIÓN ------------------------
CREDENTIALS_FILE = os.getenv("CREDENTIALS_PATH")  # Ruta del archivo de credenciales de Google en el env 
BUCKET_NAME = os.getenv("BUCKET_NAME")  # Nombre del bucket de GCS
TOKEN_PATH = os.getenv("TOKEN_PATH")  # Ruta del token de acceso
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_KEY_PATH")  # Ruta de la clave de servicio

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']  # Alcance para leer correos (solo lectura en Gmail)
FILTER_SUBJECT = 'Archivos adjuntos: presupuesto 2025 - ventas 2024 y 2025'  # Asunto de los correos a filtrar
FILTER_SENDER = 'esteban03co@gmail.com'

TEMP_DIR = 'temp_files'  # Directorio temporal para guardar archivos adjuntos, lo crea si no existe
os.makedirs(TEMP_DIR, exist_ok=True)

# ------------------------ CONFIGURACIÓN DE LOGGING ------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------------ FUNCIONES PRINCIPALES ------------------------

def gmail_authenticate():
    """Autenticarse con Gmail usando el flujo de OAuth2 y las credenciales almacenadas."""
    creds = None
    try:
        # Si existe un token guardado, lo cargamos
        if os.path.exists(TOKEN_PATH):
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        
        # Si las credenciales no son válidas, refrescarlas o pedir nuevas
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())  # Refrescar el token si está caducado
            else:
                # Si no se tienen credenciales válidas, pedir autorización
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)  # Iniciar flujo de autenticación en servidor local
            
            # Guardar el token para evitar pedir autorización de nuevo
            with open(TOKEN_PATH, 'w') as token:
                token.write(creds.to_json())
        
        logger.info("Autenticación exitosa con Gmail.")
        return build('gmail', 'v1', credentials=creds)  # Crear el servicio de Gmail
    
    except Exception as e:
        logger.error(f"Error al autenticar con Gmail: {str(e)}")
        raise

def search_emails(service, subject=None, sender=None):
    """Buscar correos electrónicos en Gmail según filtros de asunto o remitente."""
    try:
        query = ''
        if subject:
            query += f'subject:{subject} '  # Filtrar por asunto
        if sender:
            query += f'from:{sender} '  # Filtrar por remitente
        
        # Ejecutar la búsqueda en Gmail con los filtros especificados
        results = service.users().messages().list(userId='me', q=query).execute()
        logger.info(f"Se encontraron {len(results.get('messages', []))} mensajes.")
        return results.get('messages', [])  # Retornar los mensajes encontrados
    
    except Exception as e:
        logger.error(f"Error al buscar correos: {str(e)}")
        raise

def download_attachments(service, message_id):
    """Descargar los archivos adjuntos de un correo electrónico dado su ID."""
    try:
        # Obtener el mensaje completo en formato 'raw'
        message = service.users().messages().get(userId='me', id=message_id, format='raw').execute()
        msg_bytes = base64.urlsafe_b64decode(message['raw'])  # Decodificar el mensaje en base64
        mime_msg = message_from_bytes(msg_bytes)  # Parsear el mensaje MIME

        attachments = []
        # Recorrer todas las partes del mensaje (por si tiene varios adjuntos)
        for part in mime_msg.walk():
            # Ignorar las partes multipart y las que no son adjuntos
            if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
                continue
            filename = part.get_filename()  # Obtener el nombre del archivo adjunto
            if filename:
                filepath = os.path.join(TEMP_DIR, filename)  # Crear la ruta del archivo en el directorio temporal
                with open(filepath, 'wb') as f:
                    f.write(part.get_payload(decode=True))  # Escribir el contenido del archivo en disco
                attachments.append(filepath)  # Agregar el archivo a la lista de adjuntos
        logger.info(f"Se descargaron {len(attachments)} archivos adjuntos.")
        return attachments  # Retornar la lista de archivos descargados
    
    except Exception as e:
        logger.error(f"Error al descargar los adjuntos del correo {message_id}: {str(e)}")
        raise

def upload_to_gcs(local_filepath, bucket_name=BUCKET_NAME):
    """Subir un archivo desde el sistema local a Google Cloud Storage (GCS)."""
    try:
        storage_client = storage.Client()  # Crear el cliente de GCS
        bucket = storage_client.bucket(bucket_name)  # Obtener el bucket de GCS
        filename = os.path.basename(local_filepath)  # Obtener el nombre del archivo
        blob = bucket.blob(filename)  # Crear un objeto blob en GCS con el nombre del archivo
        blob.upload_from_filename(local_filepath)  # Subir el archivo a GCS
        logger.info(f"Archivo '{filename}' subido a GCS.")  # Confirmación de que el archivo fue subido
    
    except Exception as e:
        logger.error(f"Error al subir el archivo {local_filepath} a GCS: {str(e)}")
        raise
