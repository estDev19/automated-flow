from dotenv import load_dotenv
load_dotenv()
from main_ingest import ingest_from_gmail

def main():
    print("Iniciando orquestador principal...\n")

    # Ejecutar el pipeline de ingesta
    ingest_from_gmail()

    print("\nPipeline de orquestación completado.")

if __name__ == '__main__':
    main()
