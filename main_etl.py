from dotenv import load_dotenv
load_dotenv()

from etl.extract.gcs_reader import read_excel_from_gcs
from etl.transform.ventas_transform import load_and_clean_ventas

def extract_data():
    print("Iniciando extracción de datos...")

    df_presupuesto = read_excel_from_gcs("PPTO CAM 2025.xlsx")  # Carga el archivo para presupuesto * pendiente limpiar

    df_ventas = load_and_clean_ventas("VENTAS CAM 2024 - 2025.xlsx")  # Carga y limpieza del archivo de ventas

    print("Datos extraídos y limpiados correctamente.")
    return df_presupuesto, df_ventas

if __name__ == '__main__':
    # Ejecuta la extracción de datos y mostramos los resultados
    df_presupuesto, df_ventas = extract_data()
    # Muestra los datos de ventas limpios en consola
    print("Datos de ventas limpios:")
    print(df_ventas.head())  # Mostrar las primeras filas de los ventas limpios
    # Muestra los datos de presupuesto en consola
    print("Datos de presupuesto:")
    print(df_presupuesto.head())  # Muestra las primeras filas de los presupuesto
