from traceback import print_tb

import pandas as pd
from loguru import logger
from pathlib import Path

# Obtener la ruta base del proyecto
archivo_actual = Path(__file__).resolve()
raiz_proyecto = archivo_actual.parent

def fechas_archivo(path):
    """
    Obtiene las fechas mínima y máxima de un archivo CSV.

    Args:
        path: Ruta al archivo CSV (relativa o absoluta)

    Returns:
        tupla: (fecha_minima, fecha_maxima) en formato YYYY-MM-DD
    """
    # Convertir ruta relativa a absoluta si es necesario
    path_obj = Path(path)
    if not path_obj.is_absolute():
        path_obj = raiz_proyecto / path_obj

    logger.info(f'Leyendo archivo: {path_obj}')

    df = pd.read_csv(str(path_obj))

    # Convertir a datetime, pero convirtiendo errores en NaT
    df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")

    # Eliminar filas inválidas
    df = df.dropna(subset=["Fecha"])

    # Evitar romper si todo el archivo tiene errores
    if df.empty:
        return None, None

    min_fecha = df["Fecha"].min()
    max_fecha = df["Fecha"].max()

    min_fecha = str(min_fecha)[:10]
    max_fecha = str(max_fecha)[:10]

    logger.info(f'Fecha mínima: {min_fecha}, Fecha máxima: {max_fecha}')

    return min_fecha, max_fecha
