import logging

# Configura el logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def compare_forecast_vs_sales(df_sales_forecast, df_sales):
    """
    Compara presupuesto vs ventas netas reales para el año 2025.
    Calcula diferencias absolutas y porcentaje de cumplimiento tanto en USD como en kilos.

    Parámetros:
        df_sales_forecast (pd.DataFrame): Datos agregados de presupuesto.
        df_sales (pd.DataFrame): Datos agregados de ventas.

    Retorna:
        pd.DataFrame: DataFrame con métricas de comparación.
    """
    try:
        # Filtrar solo año 2025 en ventas
        df_sales_2025 = df_sales[df_sales["ano"] == 2025]
        logging.info(f"Ventas 2025: {len(df_sales_2025)} registros.")

        key_columns = [
            "ano", "mes_numero", "pais", "negocio",
            "categoria", "marca", "sub_marca",
            "codigo_material", "descripcion_material"
        ]

        # Verificar existencia de columnas clave en ambos DF
        for col in key_columns:
            if col not in df_sales_forecast.columns or col not in df_sales_2025.columns:
                raise KeyError(f"Falta columna clave en los datos: {col}")

        # Verificar cuántas coincidencias hay antes del merge
        df_temp = df_sales_forecast.merge(df_sales_2025, on=key_columns, how="inner")
        logging.info(f"Coincidencias encontradas antes del merge: {len(df_temp)}")

        # Merge principal para comparación
        df_compared = df_sales_forecast.merge(
            df_sales_2025,
            how="left",
            on=key_columns,
            suffixes=("_ppto", "_real")
        )

        # Limpieza de columnas duplicadas
        if "mes_nombre_real" in df_compared.columns:
            df_compared.drop(columns=["mes_nombre_real"], inplace=True)
        df_compared.rename(columns={"mes_nombre_ppto": "mes_nombre"}, inplace=True)

        # === CÁLCULO DE MÉTRICAS ===

        # Diferencias absolutas
        df_compared["diferencia_usd_neta"] = (
            df_compared["venta_neta_usd"] - df_compared["ppto_usd"]
        )
        df_compared["diferencia_kilos_neta"] = (
            df_compared["venta_neta_kilos"] - df_compared["ppto_kg"]
        )

        # Porcentaje de cumplimiento
        df_compared["porcentaje_cumplimiento_usd_neto"] = df_compared.apply(
            lambda x: 0 if x["ppto_usd"] == 0 else x["venta_neta_usd"] / x["ppto_usd"],
            axis=1
        )
        df_compared["porcentaje_cumplimiento_kilos_neto"] = df_compared.apply(
            lambda x: 0 if x["ppto_kg"] == 0 else x["venta_neta_kilos"] / x["ppto_kg"],
            axis=1
        )

        # Redondear los resultados
        df_compared["porcentaje_cumplimiento_usd_neto"] = df_compared["porcentaje_cumplimiento_usd_neto"].round(4)
        df_compared["porcentaje_cumplimiento_kilos_neto"] = df_compared["porcentaje_cumplimiento_kilos_neto"].round(4)

        # Diagnóstico: datos sin ventas
        sin_ventas = df_compared["venta_neta_usd"].isna().sum()
        logging.warning(f"Filas sin datos de ventas: {sin_ventas}")

        # Completar NaNs con ceros
        df_compared.fillna(0, inplace=True)

        return df_compared

    except Exception as e:
        logging.error(f"Error al comparar presupuesto vs ventas: {e}")
        raise

