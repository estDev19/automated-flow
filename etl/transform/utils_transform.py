import pandas as pd

# ------------------------ EDA ------------------------

def check_missing_values(df: pd.DataFrame):
    # Devuelve la cantidad de valores faltantes por columna.
    return df.isnull().sum()

def check_outliers_iqr(df: pd.DataFrame, columns: list):
    # Detecta outliers utilizando el método IQR (Interquartile Range).
    outlier_summary = {}
    for col in columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        outliers = df[(df[col] < lower) | (df[col] > upper)]
        outlier_summary[col] = len(outliers)
    return outlier_summary

def detect_duplicates(df: pd.DataFrame, subset=None):
    # Detecta duplicados en el dataframe y devuelve su conteo.
    return df.duplicated(subset=subset).sum()

def describe_data(df: pd.DataFrame):
    # Muestra estadísticas descriptivas básicas.
    return df.describe()

# ------------------------ LIMPIEZA ------------------------

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

# -------------------- ESTANDARIZACIÓN ------------------------
def harmonize_column_types(df):
    """
    Fuerza tipos de datos consistentes en columnas clave compartidas entre datasets.
    """
    columnas_tipo_str = [
        "mes_nombre", "país", "negocio", "categoría",
        "marca", "sub_marca", "código_material", "descripción_material"
    ]
    columnas_tipo_int = ["año", "mes_número"]
    columnas_tipo_float = ["ppto_usd", "ppto_kg", "venta_bruta_usd", "venta_neta_usd", "venta_neta_kilos", "devoluciones_usd", "descuento_usd", "porcentaje_descuento", "porcentaje_devoluciones"]

    # Convertir columnas de tipo string a minúsculas y eliminar espacios
    for col in columnas_tipo_str:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()

    # Convertir columnas de tipo int
    for col in columnas_tipo_int:
        if col in df.columns:
            # Asegúrate de que los valores nulos sean tratados correctamente
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(0).astype("Int64")  # Rellenar nulos con 0 o el valor adecuado

    # Convertir columnas de tipo float
    for col in columnas_tipo_float:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df