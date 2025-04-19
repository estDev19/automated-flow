import logging
from dotenv import load_dotenv
from etl.transform.ventas_transform import load_and_clean_sales
from etl.transform.presupuesto_transform import load_and_clean_sales_forecast
from etl.transform.comparar_presupuesto_vs_ventas import compare_forecast_vs_sales
from etl.load.load_to_bigquery import load_sales_data, load_comparison_data
from utils_log import setup_logger

# Cargar variables de entorno
load_dotenv()


# Configurar el logger
logger = setup_logger()


def extract_data():
    logging.info("Iniciando extracción y limpieza de datos...")

    df_presupuesto = load_and_clean_sales_forecast("PPTO CAM 2025.xlsx")
    df_ventas = load_and_clean_sales("VENTAS CAM 2024 - 2025.xlsx")

    logging.info("Datos extraídos y limpiados correctamente.")
    return df_presupuesto, df_ventas

def validate_columns(df_presupuesto, df_ventas):
    """Valida que las columnas numéricas esenciales estén presentes."""
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
            logging.error(f"{error}")
        raise ValueError("Errores en columnas requeridas detectados. Revisa la estructura de tus archivos.")

    logging.info("Validación de columnas numéricas completada con éxito.")

if __name__ == '__main__':
    # Extracción y limpieza
    df_presupuesto, df_ventas = extract_data()

    # Validación previa de columnas necesarias
    validate_columns(df_presupuesto, df_ventas)

    # Comparación de presupuesto vs ventas
    df_comparado = compare_forecast_vs_sales(df_presupuesto, df_ventas)

    # Vista previa
    logging.info("Vista previa de comparación:")
    logging.info(df_comparado.head().to_string(index=False))

    # Carga a BigQuery
    logging.info("Cargando datos a BigQuery...")

    # Cargar ventas y presupuesto
    load_sales_data(df_ventas)  # Cargar ventas
    load_comparison_data(df_comparado)  # Cargar comparado (presupuesto vs ventas)

    logging.info("Carga a BigQuery completada con éxito.")

