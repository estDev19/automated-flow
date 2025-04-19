from dotenv import load_dotenv
import os
from utils_log import setup_logger

# Cargar variables de entorno
load_dotenv()

# ------------------------------
# Módulos de Ingesta
# ------------------------------
from ingest.gmail_to_gcs import (
    gmail_authenticate,
    search_emails,
    download_attachments,
    upload_to_gcs
)

# ------------------------------
# Módulos de ETL
# ------------------------------
from etl.transform.ventas_transform import load_and_clean_sales
from etl.transform.presupuesto_transform import load_and_clean_sales_forecast
from etl.transform.comparar_presupuesto_vs_ventas import compare_forecast_vs_sales
from etl.load.load_to_bigquery import load_sales_data, load_comparison_data

# Configurar el logger
logger = setup_logger()

# ------------------------------
# Función de Ingesta
# ------------------------------
def run_ingest():
    logger.info("Iniciando la autenticación en Gmail...")
    gmail_service = gmail_authenticate()
    logger.info("Autenticación exitosa en Gmail.")

    messages = search_emails(gmail_service, subject='Probando', sender='esteban03co@gmail.com')

    if not messages:
        logger.warning("No se encontraron correos que coincidan con los filtros.")
        return

    for msg in messages:
        logger.info(f"Procesando el correo con ID: {msg['id']}")
        attachments = download_attachments(gmail_service, msg['id'])
        for filepath in attachments:
            try:
                logger.info(f"Subiendo archivo {filepath} a GCS...")
                upload_to_gcs(filepath)
                os.remove(filepath)
                logger.info(f"Archivo {filepath} subido y eliminado localmente.")
            except Exception as e:
                logger.error(f"Error al subir el archivo {filepath} a GCS: {str(e)}")

    logger.info("Ingesta completada exitosamente.")

# ------------------------------
# Funciones de ETL
# ------------------------------
def extract_data():
    logger.info("Iniciando extracción y limpieza de datos...")
    df_presupuesto = load_and_clean_sales_forecast("PPTO CAM 2025.xlsx")
    df_ventas = load_and_clean_sales("VENTAS CAM 2024 - 2025.xlsx")
    logger.info("Datos extraídos y limpiados correctamente.")
    return df_presupuesto, df_ventas

def validate_columns(df_presupuesto, df_ventas):
    columnas_necesarias = {
        "presupuesto": ["ppto_usd", "ppto_kg"],
        "ventas": ["venta_neta_usd", "venta_neta_kilos"]
    }

    errores = []
    for col in columnas_necesarias["presupuesto"]:
        if col not in df_presupuesto.columns:
            errores.append(f"Falta la columna {col} en presupuesto.")
    for col in columnas_necesarias["ventas"]:
        if col not in df_ventas.columns:
            errores.append(f"Falta la columna {col} en ventas.")

    if errores:
        for error in errores:
            logger.error(error)
        raise ValueError("Errores en columnas requeridas detectados.")

    logger.info("Validación de columnas completada con éxito.")

def run_etl():
    df_presupuesto, df_ventas = extract_data()
    validate_columns(df_presupuesto, df_ventas)

    df_comparado = compare_forecast_vs_sales(df_presupuesto, df_ventas)

    logger.info("Cargando datos a BigQuery...")
    load_sales_data(df_ventas)
    load_comparison_data(df_comparado)
    logger.info("Carga a BigQuery completada con éxito.")

# ------------------------------
# Punto de entrada principal
# ------------------------------
if __name__ == '__main__':
    logger.info("Inicio del pipeline completo: Ingesta + ETL")
    run_ingest()
    run_etl()
    logger.info("Pipeline completo finalizado con éxito.")
