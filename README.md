Autor: Esteban Castro Oviedo. Fecha: Abril 2025.
Automatización y Análisis de Datos Comerciales

Descripción del Proyecto:
-Este proyecto automatiza la extracción, procesamiento y visualización de datos comerciales con el objetivo de facilitar la toma de decisiones estratégicas. Se utiliza una arquitectura modular basada en Python, Google Cloud y Looker Studio para procesar datos de ventas y presupuestos, generar KPIs clave y presentar información ejecutiva a través de dashboards interactivos.

Tecnologías Utilizadas:
-Python: Para el procesamiento de datos ETL y automatización de tareas.
-Google Cloud: Utilizado para almacenamiento (Google Cloud Storage) y procesamiento (BigQuery).
-Looker Studio: Para la creación de dashboards interactivos.
-OAuth 2.0: Para la autenticación segura con Gmail.
-Git: Para el versionamiento de código y seguimiento de cambios.
-Google Cloud IAM: Para la gestión de permisos y acceso seguro.

Componentes del Proyecto
1. Extracción Automatizada de Datos desde Gmail
Objetivo: Automatizar la descarga de archivos adjuntos desde correos electrónicos específicos (por ejemplo, de ventas y presupuestos) y almacenarlos en Google Cloud Storage.
Tecnologías: OAuth 2.0, Google Cloud Storage (GCS).
Archivos relevantes:
-gmail_to_gcs.py: Extrae los correos con archivos adjuntos y los guarda en Google Cloud.

2. Procesamiento ETL Modular y Escalable
Objetivo: Procesar y transformar los datos obtenidos, limpiándolos, validándolos, y preparándolos para su análisis.
Tecnologías: Python (pandas), Google Cloud.
Flujo: Los datos se transforman y cargan en BigQuery para su posterior análisis y visualización.

Archivos relevantes:
-ventas_transform.py: Realiza la transformación de los datos de ventas.
-presupuesto_transform.py: Realiza la transformación de los datos de presupuesto.
-gcs_reader.py: Permite la lectura de datos desde Google Cloud Storage.
-comparar_presupuesto_vs_ventas.py: Compara los datos de presupuesto contra las ventas y realiza análisis adicionales.

3. Carga Dinámica y Visualización Ejecutiva
Objetivo: Cargar los datos procesados a Google BigQuery y visualizarlos en dashboards interactivos en Looker Studio.
Tecnologías: Looker Studio, Google Cloud.
Archivos relevantes:
-load_to_bigq.py: Funciones para cargar los datos procesados a BigQuery.

4. Seguridad y Buenas Prácticas DevSecOps
Objetivo: Asegurar la protección de los datos y credenciales, y aplicar buenas prácticas de desarrollo seguro.
Tecnologías: Uso de variables de entorno cifradas y permisos mínimos en servicios de Google Cloud.
Estructura del Proyecto: 

├── ingest/
│   └── gmail_to_gcs.py               # Script para extraer datos desde Gmail y subir a GCS
├── secrets/
│   └── credentials.json              # Credenciales seguras para la autenticación (no subir a Git)
├── temp_files/
│   └── temp files *                 # Guarda de forma momentánea los archivos antes de subirlos a GCS
├── logs/
│   └── etl_pipeline.log             # Archivo de logging con información de monitoreo de todo el pipeline
├── etl/
│   ├── extract/
│   │   └── gcs_reader.py            # Lectura de datos desde GCS
│   ├── transform/
│   │   ├── ventas_transform.py      # Transformación de datos de ventas
│   │   ├── presupuesto_transform.py # Transformación de datos de presupuesto
│   │   └── comparar_presupuesto_vs_ventas.py # Comparación entre ventas y presupuesto
│   └── load/
│       └── load_to_bigquery.py          # Carga de datos a Google BigQuery
├── main_ingest.py                   # Orquestador para la ingestión de datos
├── main_etl.py                       # Orquestador para el procesamiento ETL
├── main.py                           # Orquestador general
├── .env                         # Archivo con configuración de ambiente
├── requirements.txt                         # Archivo con librerías utilizadas


Manejo de Errores y Logs:
Los logs se configuran en utils_log.py, y cada parte del sistema tiene un monitoreo adecuado de logs para detectar errores y alertar sobre posibles problemas durante el proceso