from gmail_to_gcs import (
    gmail_authenticate,
    search_emails,
    download_attachments,
    upload_to_gcs
)
import os

def main():
    gmail_service = gmail_authenticate()
    print("Autenticación exitosa en Gmail.")
    messages = search_emails(gmail_service, subject='Probando', sender='esteban03co@gmail.com')
    
    if not messages:
        print("No se encontraron correos que coincidan con los filtros.")
        return

    for msg in messages:
        attachments = download_attachments(gmail_service, msg['id'])
        for filepath in attachments:
            upload_to_gcs(filepath)
            os.remove(filepath)  # Limpieza local

if __name__ == '__main__':
    main()
