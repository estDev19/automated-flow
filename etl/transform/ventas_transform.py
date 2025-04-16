from etl.extract.gcs_reader import read_excel_from_gcs
import pandas as pd

# Importa funciones de limpieza de datos de utils
from etl.transform.utils_transform import (
    remove_unwanted_columns, 
    standardize_headers, 
    remove_empty_rows, 
    remove_duplicates, 
)

def load_and_clean_ventas(filename):
    """Carga y limpia el archivo VENTAS CAM 2024 - 2025.xlsx desde GCS."""
    # Lee todo sin encabezado para poder manipular libremente
    df = read_excel_from_gcs(filename)

    # Corta las 3 primeras filas y la primera columna (índice 0)
    raw_data = df.iloc[3:, 1:]
    raw_headers = df.iloc[2, 1:]

    # Estandariza los encabezados
    cleaned_headers = standardize_headers(raw_headers)

    # Crea nuevo DataFrame limpio
    df_limpio = pd.DataFrame(data=raw_data.values, columns=cleaned_headers).reset_index(drop=True)

    # Aplica funciones de limpieza adicionales
    df_limpio = remove_empty_rows(df_limpio)
    df_limpio = remove_duplicates(df_limpio)
    df_limpio = remove_unwanted_columns(df_limpio, columns_to_remove=["unnamed:_1", "nan"])

    return df_limpio