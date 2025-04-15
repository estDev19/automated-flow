import os
import base64
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.cloud import storage
from email import message_from_bytes
from dotenv import load_dotenv

# ------------------------ CONFIGURACIÓN ------------------------
load_dotenv()

CREDENTIALS_FILE = os.getenv("CREDENTIALS_PATH")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TOKEN_PATH = os.getenv("TOKEN_PATH")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_KEY_PATH")

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
FILTER_SUBJECT = 'Probando'
FILTER_SENDER = 'esteban03co@gmail.com'

TEMP_DIR = 'temp_files'
os.makedirs(TEMP_DIR, exist_ok=True)

# ------------------------ FUNCIONES PRINCIPALES ------------------------

def gmail_authenticate():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def search_emails(service, subject=None, sender=None):
    query = ''
    if subject:
        query += f'subject:{subject} '
    if sender:
        query += f'from:{sender} '
    results = service.users().messages().list(userId='me', q=query).execute()
    return results.get('messages', [])

def download_attachments(service, message_id):
    message = service.users().messages().get(userId='me', id=message_id, format='raw').execute()
    msg_bytes = base64.urlsafe_b64decode(message['raw'])
    mime_msg = message_from_bytes(msg_bytes)

    attachments = []
    for part in mime_msg.walk():
        if part.get_content_maintype() == 'multipart' or part.get('Content-Disposition') is None:
            continue
        filename = part.get_filename()
        if filename:
            filepath = os.path.join(TEMP_DIR, filename)
            with open(filepath, 'wb') as f:
                f.write(part.get_payload(decode=True))
            attachments.append(filepath)
    return attachments

def upload_to_gcs(local_filepath, bucket_name=BUCKET_NAME):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    filename = os.path.basename(local_filepath)
    blob = bucket.blob(filename)
    blob.upload_from_filename(local_filepath)
    print(f"Archivo '{filename}' subido a GCS.")