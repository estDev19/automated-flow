from etl.extract.gcs_reader import read_excel_from_gcs
import pandas as pd

# Importa funciones de limpieza de datos de utils
from etl.transform.utils_transform import (
    remove_unwanted_columns, 
    standardize_headers, 
    remove_empty_rows, 
    remove_duplicates, 
)

def load_and_clean_presupuesto(filename):
    """
    Carga el archivo de presupuesto desde GCS con encabezados correctos
    y aplica limpieza básica sin eliminar filas de encabezado ni datos útiles.
    """
    # Leer el archivo desde GCS (la primera fila ya se considera encabezado)
    df = read_excel_from_gcs(filename)

    # Estandariza encabezados 
    df.columns = standardize_headers(df.columns)

    # Aplicar limpieza
    df_cleaned = remove_empty_rows(df)
    df_cleaned = remove_duplicates(df_cleaned)
    df_cleaned = remove_unwanted_columns(df_cleaned, columns_to_remove=["unnamed:_1", "nan"])

    return df_cleaned