import logging
import os

# Crear directorio de logs si no existe
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configuración de logging
def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Configurar el handler para la consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Configurar el handler para el archivo
    file_handler = logging.FileHandler('logs/etl_pipeline.log')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Agregar los handlers al logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
