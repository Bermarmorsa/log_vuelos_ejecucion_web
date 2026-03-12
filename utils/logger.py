import sys
from loguru import logger
from pathlib import Path

def inicializar_logger():
    """Configuración global de Loguru"""
    logger.remove() # Limpiar configuración por defecto

    # Consola: Formato elegante y nivel INFO
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level="INFO"
    )

    # Archivo: Guardar todo (DEBUG) con rotación
    log_file = Path("logs/ejecucion.log")
    logger.add(
        log_file,
        rotation="1 day",
        retention="10 days",
        level="DEBUG",
        compression="zip"
    )