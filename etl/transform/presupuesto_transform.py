import logging
from etl.extract.gcs_reader import read_excel_from_gcs

# Importa funciones de limpieza de datos de utils
from etl.transform.utils_transform import (
    harmonize_column_types,
    remove_unwanted_columns, 
    standardize_headers, 
    remove_empty_rows, 
    remove_duplicates,
    describe_data,
    check_missing_values,
    detect_duplicates
)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Diccionario de conversión de nombres de meses en español a número
MONTHS_ES = {
    "Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4,
    "Mayo": 5, "Junio": 6, "Julio": 7, "Agosto": 8,
    "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12
}


def aggregate_and_reconcile_sales_forecast(df):
    """
    Agrupa los datos de presupuesto por claves relevantes y suma los valores numéricos.
    Convierte nombres de mes a número, añade año fijo y valida columnas clave.

    Parámetros:
        df (pd.DataFrame): DataFrame limpio con columnas esperadas.

    Retorna:
        pd.DataFrame: Presupuesto agrupado por clave compuesta.
    """
    try:
        df = df.copy()

        # Renombrar "mes" a "mes_nombre" para homogeneidad
        df = df.rename(columns={"mes": "mes_nombre"})
        df["mes_nombre"] = df["mes_nombre"].str.strip()

        # Conversión a número de mes
        df["mes_numero"] = df["mes_nombre"].map(MONTHS_ES)

        # Año constante (por ahora solo hay datos 2025)
        df["ano"] = 2025

        # Validación de columnas clave
        columnas_necesarias = [
            "ano", "mes_numero", "mes_nombre", "pais", "negocio",
            "categoria", "marca", "sub_marca", "codigo_material",
            "descripcion_material", "ppto_usd", "ppto_kg"
        ]
        faltantes = [col for col in columnas_necesarias if col not in df.columns]
        if faltantes:
            raise ValueError(f"Faltan columnas en el archivo de presupuesto: {faltantes}")

        # Filtrado de registros inválidos
        df = df[
            (df["ppto_usd"] > 0) &
            (df["ppto_kg"] > 0) &
            (df["mes_numero"].notna())
        ]

        # Agrupación
        df_grouped = df.groupby(
            [
                "ano", "mes_numero", "mes_nombre", "pais", "negocio",
                "categoria", "marca", "sub_marca",
                "codigo_material", "descripcion_material"
            ],
            as_index=False
        ).agg({
            "ppto_usd": "sum",
            "ppto_kg": "sum"
        })

        logging.info("Presupuesto agregado correctamente.")
        return df_grouped

    except Exception as e:
        logging.error(f"Error al agregar presupuesto: {e}")
        raise


def load_and_clean_sales_forecast(filename):
    """
    Carga el archivo de presupuesto desde GCS, estandariza encabezados,
    limpia datos innecesarios y retorna DataFrame agregado por clave.

    Parámetros:
        filename (str): Ruta del archivo en GCS.

    Retorna:
        pd.DataFrame: Presupuesto limpio y listo para análisis o comparación.
    """
    try:
        logging.info(f"Cargando archivo de presupuesto desde GCS: {filename}")
        df = read_excel_from_gcs(filename)

        # Estandarización de encabezados
        df.columns = standardize_headers(df.columns)

        # Armonización de tipos antes de filtrar/limpiar
        df = harmonize_column_types(df)

        # Limpieza básica
        df_cleaned = remove_empty_rows(df)
        df_cleaned = remove_duplicates(df_cleaned)
        df_cleaned = remove_unwanted_columns(df_cleaned, columns_to_remove=["unnamed:_1", "nan"])

        # Agregación
        df_grouped = aggregate_and_reconcile_sales_forecast(df_cleaned)

        # Chequeos EDA
        logging.info("\nAnálisis EDA de datos de presupuesto (agrupado):\n")
        logging.info("Valores nulos por columna:\n" + f"\n{check_missing_values(df_grouped)}\n")
        logging.info("Cantidad de registros duplicados:\n" + f"\n{detect_duplicates(df_grouped)}\n")
        logging.info("Estadísticas descriptivas de columnas numéricas:\n" + f"\n{describe_data(df_grouped)}\n")

        return df_grouped

    except Exception as e:
        logging.error(f"Error al procesar el archivo de presupuesto: {e}")
        raise
