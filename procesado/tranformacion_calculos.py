import pandas as pd
from loguru import logger
import calculos_aux
from pathlib import Path
import datetime
import os

# Obtener la ruta base del proyecto
archivo_actual = Path(__file__).resolve()
raiz_proyecto = archivo_actual.parent.parent
logger.info('-------------raiz_proyecto--------------')
logger.info(raiz_proyecto)

hoy = datetime.datetime.now().date()


def transformaciones_log(ruta_path):


    df_log = pd.read_csv(str(ruta_path))

    #extrar del csv las celdas que nos interesan tipo vuelo I y no vacios

    df_log_i = df_log[df_log["tipo vuelo"].fillna("").str.strip() == "I"]


    df_log_lf = df_log_i[['tipo vuelo','Fecha','Hora inicio','Hora Fin','Avion','Aerodromo Origen','Aerodromo Destino','Observaciones','Precio Hora']].copy()

    #pasar los datos de horas de inicio y fin a fecha
    df_log_lf['Fecha'] = pd.to_datetime(df_log_lf['Fecha'], errors='coerce')

    print(df_log_lf.dtypes)
    df_log_lf.loc[:,'Fecha'] = pd.to_datetime(df_log_lf['Fecha'], errors='coerce',dayfirst=False)

    if pd.api.types.is_object_dtype(df_log_lf['Hora inicio']):
        df_log_lf['Hora inicio'] = pd.to_datetime(df_log_lf['Fecha'].dt.strftime('%Y-%m-%d') + ' ' + df_log_lf['Hora inicio'].astype(str), errors='coerce')

    # Check if 'Hora Fin' is still an object type before converting
    if pd.api.types.is_object_dtype(df_log_lf['Hora Fin']):
        df_log_lf['Hora Fin'] = pd.to_datetime(df_log_lf['Fecha'].dt.strftime('%Y-%m-%d') + ' ' + df_log_lf['Hora Fin'].astype(str))




    #calcular las horas acumuladas por cada linea en un nuevo campo


    df_log_lf.loc[:,'Tiempo vuelo'] = df_log_lf['Hora Fin'] - df_log_lf['Hora inicio']

    df_log_lf.loc[:,'tiempo acumulado'] = df_log_lf['Tiempo vuelo'].cumsum()

    df_log_lf.loc[:,"acumulado_decimales"] = df_log_lf["tiempo acumulado"].dt.total_seconds() / 3600





    #crear nuevo campo con el calculo de coste horas_acumuladas * precio

    df_log_lf["coste"] = df_log_lf["Precio Hora"] * df_log_lf["acumulado_decimales"]


    #crear nuevo campo que sea el calculo de frecuencia de vuelo para cada linea, horas de vuelo / dias trascurridos de instrucción

    min_fecha = df_log_lf["Fecha"].min()

    #print(min_fecha)

    #df_log_lf['frecuencia_vuelos'] = df_log_lf["acumulado_decimales"] / (df_log_lf["Fecha"] - min_fecha )

    df_log_lf['frecuencia_vuelos'] = round((df_log_lf["Fecha"] - min_fecha ).dt.days / df_log_lf["acumulado_decimales"],2)

    logger.info('dataframe de log')
    print(df_log_lf)

    return df_log_lf


    #obtener datos de meteo en nuevas columnas los datos de la meteo.

def datos_meteo(df, ruta):
    """
    Carga los datos de meteorología desde el archivo parquet generado.

    Args:
        df: DataFrame con los datos del log
        ruta: Ruta al archivo CSV del log de vuelos

    Returns:
        DataFrame con los datos de meteorología
    """
    print('-----------------contenido df log para buscar fechas min y max-------------------')
    print(df)

    fecha_inicio_to_met , fecha_fin_to_met = calculos_aux.fechas_archivo(ruta)

    nombre_archivo = f'meteo_open_{fecha_inicio_to_met[0:10]}_{fecha_fin_to_met[0:10]}.parquet'

    nombre_archivo_meteo = raiz_proyecto / "procesado" / "save_data" / nombre_archivo

    logger.info('-----------------nombre_archivo_meteo-------------------')
    logger.info(f'{nombre_archivo_meteo}')

    #cargamos el df de meteo.
    df_meteo = pd.read_parquet(str(nombre_archivo_meteo))

    return df_meteo

def join_log_meteo(ruta):

    # Convertir ruta relativa a ruta absoluta si es necesario
    subcarpeta = 'data_in'
    subcarpeta = Path(subcarpeta)
    ruta = Path(subcarpeta / ruta )
    ruta_path = Path(ruta)
    if not ruta_path.is_absolute():
        ruta_path = raiz_proyecto / ruta_path
    logger.info('-------------ruta_path--------------')
    logger.info(ruta_path)

    logger.info(f'Ruta completa del archivo: {ruta_path}')
    logger.info(f'¿Existe el archivo?: {ruta_path.exists()}')

    if not ruta_path.exists():
        raise FileNotFoundError(f"El archivo no existe: {ruta_path}")


    # cracion de dataframe de log de vuelos
    df_log= transformaciones_log(ruta_path)
    logger.info('Creando el dataframe de log')

    df_log = df_log.sort_values("Hora Fin")
    logger.info('ordenamos el dataframe de log previo a join')
    print(df_log)


    #cargamos la meteo creación de la meteo de la API
    df_meteo = datos_meteo(df_log, ruta_path)

    logger.info('Carga de la Meteo')

    # Para crear la fecha como la que hay en el log

    # Mantener solo las filas donde horatmax != "Varias"
    #df_meteo = df_meteo[df_meteo["horatmax"] != "Varias"]
    try:
        df_meteo['fecha_hora'] = df_meteo["date"]
        print(f'--------------------------------formato de fecha en meteo {df_meteo["fecha_hora"]}')
        df_meteo["fecha_hora"] = pd.to_datetime(df_meteo['fecha_hora'], format="%Y-%m-%d %H:%M:%S", errors="raise")

        # fitramos los nulos
        df_meteo = df_meteo[df_meteo["fecha_hora"].notna()]
        # antes del cruce hay que ordenar los dartaframes
        df_meteo = df_meteo.sort_values("fecha_hora")
        logger.info('Filtrados de meteo y orden previo a join')
    except Exception as e:
        logger.info(e)

    #print(df_meteo)
    #print(df_meteo.dtypes)


    print('------------tipo log----------------------')
    print(df_log.dtypes)
    print('------------tipo meteo----------------------')
    print(df_meteo.dtypes)

    # cambiar tipo de campo de cruce en el log
    df_log["Hora Fin"] = df_log["Hora Fin"].dt.tz_localize(None)

    #cambiar fecha de cruce en meteo

    #df_meteo["fecha_hora"] = df_meteo["fecha_hora"].astype("datetime64[us]")

    df_meteo["fecha_hora"] = (
        df_meteo["fecha_hora"]
        .dt.tz_localize(None)
        .astype("datetime64[s]")  # quita sub-segundos
        .astype("datetime64[ns]")  # misma info pero en resolución us
    )

    df_meteo["date"] = (
        df_meteo["date"]
        .dt.tz_localize(None)
        .astype("datetime64[s]")  # quita sub-segundos
        .astype("datetime64[us]")  # misma info pero en resolución us
    )

    print('------------tipo log despues de cambio fecha----------------------')
    print(df_log.dtypes)
    print('------------tipo log despues de cambio fecha----------------------')
    print(df_meteo.dtypes)
    print('------------tipo log despues de cambio fecha fin----------------------')



    # union de los dataframes de por la fecha y hora mas cercanas para añadir los datos de estación mas proximos a la hora y dia del vuelo.
    df_resultado = pd.merge_asof(
        df_log, df_meteo,
        left_on="Hora Fin",
        right_on='fecha_hora', # o left_on/right_on si se llaman distinto
        direction="nearest"     # o 'backward'/'forward'
    )

    logger.info('Creacion de join de log y meteo')


    #creamos ruta para guardar el resultado en un parquet con el nombre log_meteo_join_fecha_actual.parquet
    nombre_archivo = f'log_meteo_join_{str(df_resultado["Fecha"].min())[:10]}_{str(df_resultado["Fecha"].max())[:10]}.parquet'
    join_path = raiz_proyecto / 'procesado' / "save_data"  / nombre_archivo
    logger.info(f'Creando el archivo de log y meteo unido: {join_path}')


    #se creara el archivo si no existe.
    if  os.path.isfile(join_path):
        logger.info('El archivo de join ya existe')
    else:
        df_resultado.to_parquet(join_path, index=False)

    return df_resultado

#solo para testear la función de unión de log y meteo, con el archivo de test que tenemos en data_in

#df_log = transformaciones_log('template_flight_log-test.csv')

#print(datos_meteo(df_log,'data_in/template_flight_log-test.csv'))


join_log_meteo('template_flight_log-test.csv')


