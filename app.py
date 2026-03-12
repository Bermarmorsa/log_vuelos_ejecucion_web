import streamlit as st
import pandas as pd
import os
from procesado import open_meteo, tranformacion_calculos
from calculos_aux import fechas_archivo
import plotly.express as px
import logging


def inicio():
    st.title("Carga de log de vuelos")
    st.write('''
    Esta es una pagina para cargar los log de tus vuelos y analizar los datos.
    Esta enfocado inicialmente a vuelos de escuela, para los vuelos que se han realizado
    durante el periodo de formación, pero se puede adaptar a cualquier tipo de vuelo.
    
    Pasos a seguir:
    1. Carga tu log de vuelos en formato CSV o xlsx. (dispondrás de una plantilla para facilitar la carga de datos)
    
    2. Añadir a tus datos los datos meteorológicos para cada fecha de vuelo y en tu aeródromo:
    Para eso te pediremos la coordenadas de tu aeródromo.
    
    3. Posteriormente podrás visualizar tus datos en forma te tabla: añadiendo algunos datos calculados como:
        * Duración del vuelo
        * Horas acumuladas: Tanto de instrucción como solo.
        * Coste acumulado
        * Evolución de tu frecuencia de vuelo, cada cuantos días vuelas.
        
    4. También se podrán visualizar tus datos en forma de gráficos, para que puedas ver tu evolución a lo largo del tiempo, y comparar diferentes aspectos de tus vuelos.
  
    ''')

def carga_datos():
    st.write('Carga tu log de vuelos en formato CSV o xlsx.')
    archivo_cargado = st.file_uploader('Elige un archivo CSV', type='csv')
    if archivo_cargado is not None:
        detalle_csv = {'nombre_archivo': archivo_cargado.name,
                       'tipo_archivo': archivo_cargado.type,
                       'filas_archivo': archivo_cargado.size}
        st.write(detalle_csv)
        df = pd.read_csv(archivo_cargado)
        st.write(df.describe())
        st.dataframe(df.iloc[:20])
        guardar = st.button('Guardar archivo')

    #el archivo que se carga se tiene que guardar en la carpeta data_in, para que luego se pueda acceder a el desde las otras páginas.
        if guardar == True:
            with open(f"data_in/{archivo_cargado.name}", "wb") as f:
                f.write(archivo_cargado.getbuffer())
                st.success(f"Archivo {archivo_cargado.name} guardado en data_in")








    #disponibilizar un archivo de plantilla para facilitar la carga de datos, con las columnas necesarias para el análisis posterior.
    st.write("Plantilla para carga de datos")
    st.download_button('Descargar plantilla', data=open('data_out/template_flight_log.csv', 'rb'), file_name='template_flight_log.csv', mime='text/csv')






def meteorologia_kpi():
    st.title('Meteorología')
    st.write('Añadiremos los datos meteorológicos a nuestro log de vuelo')
    st.write('Primero necesitamos que se haya cargado el log de vuelos, para poder añadir los datos meteorológicos a ese log')
    st.write('Luego necesitaremos las coordenadas de nuestro aeródromo:')

    #elegir el archivo que se ha cargado en la página de carga de datos, para añadir los datos meteorologicos a ese archivo.
    archivos_disponibles = os.listdir('data_in')
    if len(archivos_disponibles) == 0:
        st.warning('No hay archivos cargados. Por favor, carga un archivo en la página de carga de datos.')
        return
    archivo_seleccionado = st.selectbox('Selecciona el archivo al que quieres añadir los datos meteorologicos', archivos_disponibles)
    st.session_state.archivo_seleccionado = archivo_seleccionado
    st.write(f'Has seleccionado el archivo: {archivo_seleccionado}')

    #obtenemos fecha minima y maxima del archivo seleccionado, para luego obtener los datos meteorologicos de ese periodo de tiempo.

    #st.write('-----------------archivo seleccionado-------------------')
    #st.write(type(archivo_seleccionado))

    fecha_min, fecha_max = fechas_archivo(f'data_in/{archivo_seleccionado}')



    #guardar en variables los valores de latitud y longitud que se introduzcan en los campos de texto,
    # para luego poder utilizarlos para obtener los datos meteorologicos de esa ubicación.

    latitud = st.text_input('Introduce la latitud de tu aerodromo')
    longitud = st.text_input('Introduce la longitud de tu aerodromo')
    st.info(f'latitud: {latitud} longitud: {longitud} fecha_min: {fecha_min} fecha_max: {fecha_max}')


    #llamar a funcion que va a leer el archivo y prepararlo para el cruce con el API de datos meteorologico
    download_meteo = st.button('Descargar datos meteorologicos')
    if download_meteo == True:
        st.write('Descargando datos meteorologicos...')
        #aqui se llamaria a la funcion que se encarga de leer el archivo, obtener los datos meteorologicos de la API y añadirlos al archivo.
        df_met = open_meteo.comprobar_archivo(fecha_min,fecha_max,longitud ,latitud)
        st.dataframe(df_met.head())
        if df_met is not None:
            st.success('Datos meteorológicos guardados')


    #Union de datos log con datos meteo y campos calculados

        st.title('Añadir campos calculados y datos meteorológicos')
        st.write('Se añadirán los campos calculados a nuestro log de vuelo, como por ejemplo:')
        st.write('* Duración del vuelo')
        st.write('* Horas acumuladas: Tanto de instruccion como solo.')
        st.write('* Coste acumulado')
        st.write('* Evolucion de tu frecuencia de vuelo, cada cuantos días vuelas.')
        st.write('Tambien se añadirán los datos meteorológicos a nuestro log de vuelo')

        #añadimos una pantalla info con el nombre del archivo seleccionado en la página de meteorologia,
        # para que el usuario sepa a que archivo se le van a añadir los campos calculados
        # y los datos meteorologicos.
        st.info(f'Archivo seleccionado: {st.session_state.archivo_seleccionado}')
        pass

    # añadimos un boton para que el usuario pueda elegir si quiere añadir los campos calculados
    # y los datos meteorologicos al archivo seleccionado. Al presinarlo se ejecuta la funcion
    st.session_state.add_kpi_meteo = False
    st.session_state.add_kpi_meteo = st.button('Añadir campos calculados y datos meteorologicos')
    if st.session_state.add_kpi_meteo:
        st.write('Añadiendo campos calculados y datos meteorologicos...')
        # aqui se llamaria a la funcion que se encarga de leer el archivo seleccionado, añadir los campos calculados y los datos meteorologicos, y guardar el nuevo archivo con los cambios.
        # la funcion devolveria el nuevo dataframe con los cambios para mostrarlo en pantalla.
        df_join = tranformacion_calculos.join_log_meteo(st.session_state.archivo_seleccionado)
        st.write('Este es el archivo enriquecido con los campos calculados y los datos meteorologicos:')
        st.dataframe(df_join.head())


def visualizacion_datos():
    st.title("Visualización de datos")
    st.write('''En esta página se podrán visualizar los datos de nuestro log de vuelo enriquecido 
                con los campos calculados y los datos meteorologicos, en forma de tabla y gráficos.''')
    st.write('''Primero se mostrará una tabla con los datos enriquecidos, para luego mostrar 
                diferentes gráficos que nos permitan visualizar la evolución de nuestros vuelos a lo largo
                del tiempo, y comparar diferentes aspectos de nuestros vuelos.''')

    archivos_disponibles = os.listdir('procesado/save_data')
    if len(archivos_disponibles) == 0:
        st.warning('No hay archivos cargados. Por favor, carga un archivo en la página de carga de datos.')
        return
    archivo_seleccionado = st.selectbox('Selecciona el archivo para realizar las graficas',
                                        archivos_disponibles)
    st.session_state.archivo_seleccionado = archivo_seleccionado
    st.write(f'Has seleccionado el archivo: {archivo_seleccionado}')

    #aqui se lee el archivo selecionado
    df_visualizacion = pd.read_parquet(f'procesado/save_data/{archivo_seleccionado}')
    st.dataframe(df_visualizacion)


    #selecionar la visualización
    chart_type = st.selectbox(
        'Selecciona el tipo de gráfico',
        ['Barras', 'Dispersion', 'Línea']
    )

    #selector de datos
    x_axis = st.selectbox('Selecciona el eje X', df_visualizacion.columns)
    y_axis = st.selectbox('Selecciona el eje Y', df_visualizacion.columns)

    #generar gráfico
    if chart_type == 'Barras':
        fig = px.bar(df_visualizacion, x=x_axis, y=y_axis)
    elif chart_type == 'Dispersion':
        fig = px.scatter(df_visualizacion, x=x_axis, y=y_axis)
    else:
        fig = px.line(df_visualizacion, x=x_axis, y=y_axis)
    st.plotly_chart(fig, use_container_width=True)








def main():



    pagina = st.sidebar.selectbox('Selecione una página', ['Página principal',
                                                               'Carga de archivos',
                                                               'Enriquecimiento de datos con meteo y campos calculados',
                                                               'Visualización de datos',
                                                               'Gráficos'])

    if pagina == 'Página principal':
        inicio()
    elif pagina == 'Carga de archivos':
        carga_datos()
    elif pagina == 'Enriquecimiento de datos con meteo y campos calculados':
        meteorologia_kpi()
    elif pagina == 'Visualización de datos':
        visualizacion_datos()

    else:
        st.title("Gráficos")



if __name__ == '__main__':
    main()