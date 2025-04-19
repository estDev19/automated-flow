import logging
import pandas as pd
from etl.extract.gcs_reader import read_excel_from_gcs

# Importa funciones de limpieza de datos de utils 
from etl.transform.utils_transform import (
    harmonize_column_types,
    remove_unwanted_columns, 
    standardize_headers, 
    remove_empty_rows, 
    remove_duplicates, 
    check_missing_values, 
    check_outliers_iqr, 
    detect_duplicates, 
    describe_data
)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def aggregate_and_reconcile_sales(df):
    """
    Agrupa los datos de ventas por una clave compuesta y suma métricas numéricas.
    Calcula promedios para columnas porcentuales como descuentos y devoluciones.
    
    Parámetros:
        df (pd.DataFrame): DataFrame limpio y armonizado con columnas esperadas.
    
    Retorna:
        pd.DataFrame: DataFrame con métricas agregadas por clave.
    """
    try:
        df = df.copy()

        # Filtrar registros inválidos (ventas negativas o cero)
        df = df[
            (df["venta_bruta_usd"] > 0) &
            (df["venta_neta_kilos"] > 0)
        ]

        # Calcular columnas derivadas
        df["descuento_usd"] = df["venta_bruta_usd"] - df["venta_neta_usd"]

        df["porcentaje_descuento"] = df.apply(
            lambda row: (row["descuento_usd"] / row["venta_bruta_usd"] * 100)
            if row["venta_bruta_usd"] > 0 else 0,
            axis=1
        )

        df["porcentaje_devoluciones"] = df.apply(
            lambda row: (row["devoluciones_usd"] / row["venta_bruta_usd"] * 100)
            if row["venta_bruta_usd"] > 0 else 0,
            axis=1
        )

        # Agregación por clave compuesta
        aggregated = df.groupby(
            [
                "ano", "mes_numero", "mes_nombre",
                "pais", "negocio", "categoria",
                "marca", "sub_marca",
                "codigo_material", "descripcion_material"
            ],
            as_index=False
        ).agg({
            "venta_bruta_usd": "sum",
            "venta_neta_usd": "sum",
            "venta_neta_kilos": "sum",
            "venta_neta_unidades": "sum",
            "devoluciones_usd": "sum",
            "descuento_usd": "sum",
            "porcentaje_descuento": "mean",
            "porcentaje_devoluciones": "mean"
        })

        logging.info("Agregación de datos de ventas completada.")
        return aggregated

    except Exception as e:
        logging.error(f"Error al agregar ventas: {e}")
        raise

def load_and_clean_sales(filename):
    """
    Carga un archivo Excel de ventas desde GCS, realiza limpieza y transformación,
    y retorna un DataFrame listo para análisis y comparación.

    Parámetros:
        filename (str): Ruta del archivo en el bucket de GCS.

    Retorna:
        pd.DataFrame: Datos de ventas limpios y agregados.
    """
    try:
        # Leer archivo Excel desde GCS
        logging.info(f"Cargando archivo de ventas desde GCS: {filename}")
        df = read_excel_from_gcs(filename)

        # Procesar encabezados y cuerpo de datos
        raw_data = df.iloc[3:, 1:]
        raw_headers = df.iloc[2, 1:]
        cleaned_headers = standardize_headers(raw_headers)

        df_cleaned = pd.DataFrame(data=raw_data.values, columns=cleaned_headers).reset_index(drop=True)
        logging.info("Estandarización de headers completada.")

        # Limpieza básica
        df_cleaned = remove_empty_rows(df_cleaned)
        df_cleaned = remove_duplicates(df_cleaned)
        df_cleaned = remove_unwanted_columns(df_cleaned, columns_to_remove=["unnamed:_1", "nan"])

        # Armonización de tipos de columna
        df_cleaned = harmonize_column_types(df_cleaned)
        logging.info("Limpieza y armonización completadas.")

        # Agregación y reconciliación
        df_grouped = aggregate_and_reconcile_sales(df_cleaned)

        # Chequeos EDA
        logging.info("\nAnálisis EDA de datos de ventas (agrupado):\n")
        logging.info("Valores nulos por columna:\n" + f"\n{check_missing_values(df_grouped)}\n")
        logging.info("Detección de outliers utilizando IQR para 'venta_bruta_usd' y 'venta_neta_kilos':\n" +
                    f"\n{check_outliers_iqr(df_grouped, ['venta_bruta_usd', 'venta_neta_kilos'])}\n")
        logging.info("Cantidad de registros duplicados:\n" + f"\n{detect_duplicates(df_grouped)}\n")
        logging.info("Estadísticas descriptivas de columnas numéricas:\n" + f"\n{describe_data(df_grouped)}\n")

        return df_grouped

    except Exception as e:
        logging.error(f"Error al procesar ventas: {e}")
        raise
