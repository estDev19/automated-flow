import pandas as pd

# Elimina columnas no deseadas del DataFrame
# Si alguna columna en 'columns_to_remove' no existe, la ignora
def remove_unwanted_columns(df, columns_to_remove): 
    return df.drop(columns=columns_to_remove, errors='ignore')

# Estandariza los encabezados: convierte a texto, quita espacios al inicio y final, los pasa a minúsculas y reemplaza espacios por guiones bajos
def standardize_headers(headers):
    return [str(h).strip().lower().replace(' ', '_') for h in headers]

# Elimina filas que estén completamente vacías
def remove_empty_rows(df):
    return df.dropna(how='all')

# Elimina filas duplicadas 
def remove_duplicates(df):
    return df.drop_duplicates()