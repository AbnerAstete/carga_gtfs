import sys
import os, zipfile
from os import remove
import shutil

import pandas as pd

from sqlalchemy import create_engine, desc, MetaData, func,select, text
from sqlalchemy.orm import sessionmaker
from models.models import Zip,Agency,Stop,Route,Stop_Times,Calendar,Trip
from sqlalchemy.sql import exists

#Nos conectamos a la base de datos. Base de datos llamada "gtfs". En localhost. En el puerto 5432. utilizando el usaurio "postgres" y la contraseña "123321"
engine = create_engine("postgresql://postgres:123321@localhost:5432/gtfs")
Session = sessionmaker(bind=engine)
session = Session()


#Declaramos funciones que nos ayudaran en partes especificas del proceso

def actualizar_estado_mensaje(resultado, estado, mensaje):
    resultado.estado = estado
    resultado.mensaje = mensaje
    session.commit()
    
def verificar_columnas(dataframe, columnas_necesarias, archivo):
    columnas_sobrantes = [columna for columna in dataframe.columns if columna not in columnas_necesarias]
    columnas_faltantes = [columna for columna in columnas_necesarias if columna not in dataframe.columns]

    dataframe_filtrado = dataframe.drop(columnas_sobrantes, axis=1)
    
    if len(columnas_faltantes) > 0:
        estado = "Error"
        mensaje = f"Columnas faltantes en archivo {archivo}, por favor asegúrese de que el archivo contenga las siguientes columnas: {', '.join(columnas_faltantes)}"
        actualizar_estado_mensaje(resultado,estado,mensaje)
        raise ValueError(mensaje)
    else:
        estado = "Procesando"
        mensaje = f"Archivo '{archivo}' leído y comprobado que contenga todas las columnas necesarias, siguiendo proceso..."
        actualizar_estado_mensaje(resultado,estado,mensaje)
        return dataframe_filtrado

def cargar_datos(df, tabla, engine):
    try:
        df.to_sql(tabla, engine, if_exists='append', index=False)
        estado = "Procesando"
        mensaje = f"Archivo '{tabla}.txt' ha sido cargado a la tabla, siguiendo proceso..."
        actualizar_estado_mensaje(resultado, estado, mensaje)

    except Exception as e:
        estado = "Error"
        mensaje = f"Se presentaron problemas con el Archivo '{tabla}.txt' al momento de ingresar los datos a la tabla."
        actualizar_estado_mensaje(resultado, estado, mensaje)

def eliminar_registros_antiguos(session, df_db, df,modelo,tabla):
    try:
        comparar = df_db.merge(df, indicator=True, how='outer')
        registros_antiguos = comparar.loc[lambda x: x['_merge'] == 'left_only'].drop(columns='_merge')
        for i, row in registros_antiguos.iterrows():
            session.query(modelo).filter_by(**{modelo.__table__.primary_key.columns.keys()[0]: row[0]}).delete()
            session.commit()

        estado = "Procesando"
        mensaje = f"Se eliminaron los registros obsoletos, no se encontraban en el dataframe '{tabla}' entrante."
        actualizar_estado_mensaje(resultado, estado, mensaje)

    except Exception as e:
        estado = "Error"
        mensaje = f"Se presentaron problemas con el al momento de eliminar los registros que no se encontraban en el'{tabla}' entrante."
        actualizar_estado_mensaje(resultado, estado, mensaje)
    

def agregar_registros_nuevos(df_db, df, tabla, engine):
    try:
        comparar = df_db.merge(df, indicator=True, how='outer')
        diferencias = comparar.loc[lambda x: x['_merge'] == 'right_only'].drop(columns='_merge')
        diferencias.to_sql(tabla, engine, if_exists='append', index=False)

        estado = "Procesando"
        mensaje = f"Se agregaron los registros nuevos en la tabla de '{tabla}'."
        actualizar_estado_mensaje(resultado, estado, mensaje)

    except Exception as e:
        estado = "Error"
        mensaje = f"Se presentaron problemas con el al momento de agregar registros nuevos en la tabla de '{tabla}'."
        actualizar_estado_mensaje(resultado, estado, mensaje)

def verificar_base_de_datos_vacia(session):
   
    # Verificar si las tablas tiene al menos un registro
    tiene_registros_agency = session.query(exists().select_from(Agency)).scalar()
    tiene_registros_route = session.query(exists().select_from(Route)).scalar()
    tiene_registros_calendar = session.query(exists().select_from(Calendar)).scalar()
    tiene_registros_stop = session.query(exists().select_from(Stop)).scalar()
    tiene_registros_trip = session.query(exists().select_from(Trip)).scalar()
    tiene_registros_stop_times = session.query(exists().select_from(Stop_Times)).scalar()

    # Verificar si la base de datos tiene datos
    base_de_datos_vacia = (
        not tiene_registros_agency and
        not tiene_registros_route and
        not tiene_registros_calendar and
        not tiene_registros_stop and
        not tiene_registros_trip and
        not tiene_registros_stop_times
    )
    
    return base_de_datos_vacia

#Seleccionamos el ultimo zip que fue subido, recolectamos los datos del zip y notificamos a la bd en que parte del proceso estamos

resultado = session.query(Zip).order_by(desc('fecha')).first()
id_zip = resultado.id_transaccion
nombre_zip = resultado.nombre_zip
fecha_zip = resultado.fecha

estado = "Procesando"
mensaje = "Recolectando datos del ultimo archivo ZIP ingresado."
actualizar_estado_mensaje(resultado,estado,mensaje)


#Extraigo todo los archivos txt del ultimo ZIP que se ingreso
ruta = 'zips/'
ruta_zip = ruta + nombre_zip + ".zip"

with zipfile.ZipFile(ruta_zip, 'r') as archivo_zip:
    archivo_zip.extractall('txts/')

estado = "Procesando"
mensaje = "Extrayendo archivos del ultimo ZIP ingresado."
actualizar_estado_mensaje(resultado,estado,mensaje)


# Variables que me ayudaran a inspeccionar los elementos dentro del ZIP
ruta_txt = 'txts/'
archivos_en_txt = os.listdir(ruta_txt)

#Se especifican los archivos que son necesarios
archivos_esperados = ['agency.txt','calendar.txt','routes.txt','stop_times.txt','stops.txt','trips.txt']
archivos_faltantes = []

# Inspecciono si tengo todos los archivos esperados
for archivo in archivos_esperados:
    if archivo not in archivos_en_txt:
        archivos_faltantes.append(archivo)

# Este condicional dara verdadero si no se tienen los archivos esperados/necesarios.
if len(archivos_faltantes) > 0:

    #Se actualiza el estado y el mensaje en la BD.
    estado = "Error"
    mensaje = "Archivos Faltantes, por favor ingrese los siguientes archivos: "+",".join(archivos_faltantes)
    actualizar_estado_mensaje(resultado,estado,mensaje)

    # Si el ZIP no tiene los archivos esperados se elimina del directorio.
    remove(ruta_zip)
    # Se eliminan los archivos txts.
    shutil.rmtree(ruta)
    # Controlamos la excepcion
    raise ValueError(mensaje)


# Se actualiza el estado del procesamineto del archivo en la BD.
estado = "Procesando"
mensaje = "Total de archivos esperados, siguiendo proceso..."
actualizar_estado_mensaje(resultado,estado,mensaje)

# Eliminamos los archivos sobrantes en la ruta txt
for archivo in archivos_en_txt:
    if archivo not in archivos_esperados:
        os.remove(ruta_txt+archivo)


#Leemos el archivo agency.txt, lo guardamos en un dataframe, comprobamos que tenga las columnas necesarias y actualizamos la BD
try:
    df_agency = pd.read_csv('txts/agency.txt', dtype=str)
except Exception as e:
        estado = "Error"
        mensaje = "Se presentaron problemas al momento de transformar el archivo agency.txt a dataframe"
        actualizar_estado_mensaje(resultado, estado, mensaje)
columnas_necesarias_agency = ['agency_id','agency_name','agency_url','agency_timezone','agency_lang','agency_phone','agency_fare_url']
df_agency = verificar_columnas(df_agency, columnas_necesarias_agency, 'agency.txt')

#Leemos el archivo calendar.txt , lo guardamos en un dataframe y actualizamos la BD
try:
    df_calendar = pd.read_csv('txts/calendar.txt', dtype=str)
except Exception as e:
        estado = "Error"
        mensaje = "Se presentaron problemas al momento de transformar el archivo calendar.txt a dataframe"
        actualizar_estado_mensaje(resultado, estado, mensaje)
columnas_necesarias_calendar = ['service_id','start_date','end_date','monday','tuesday','wednesday','thursday','friday','saturday','sunday']
df_calendar = verificar_columnas(df_calendar, columnas_necesarias_calendar, 'calendar.txt')

#Leemos el archivo routes.txt , lo guardamos en un dataframe y actualizamos la BD
try:
    df_routes = pd.read_csv('txts/routes.txt', dtype=str)
except Exception as e:
        estado = "Error"
        mensaje = "Se presentaron problemas al momento de transformar el archivo routes.txt a dataframe"
        actualizar_estado_mensaje(resultado, estado, mensaje)
columnas_necesarias_route = ['route_id','agency_id','route_short_name','route_long_name','route_desc','route_type','route_url','route_color','route_text_color']
df_routes = verificar_columnas(df_routes, columnas_necesarias_route, 'routes.txt')

#Leemos el archivo stop_times.txt , lo guardamos en un dataframe y actualizamos la BD
try:
    df_stop_times = pd.read_csv('txts/stop_times.txt', dtype=str)
except Exception as e:
        estado = "Error"
        mensaje = "Se presentaron problemas al momento de transformar el archivo stop_times.txt a dataframe"
        actualizar_estado_mensaje(resultado, estado, mensaje)
columnas_necesarias_stop_times = ['trip_id','arrival_time','departure_time','stop_id','stop_sequence','stop_headsign','pickup_type','drop_off_type','timepoint']
df_stop_times = verificar_columnas(df_stop_times, columnas_necesarias_stop_times, 'stop_times.txt')

#Leemos el archivo stops.txt , lo guardamos en un dataframe y actualizamos la BD
try:
    df_stops = pd.read_csv('txts/stops.txt', dtype=str)
except Exception as e:
        estado = "Error"
        mensaje = "Se presentaron problemas al momento de transformar el archivo stops.txt a dataframe"
        actualizar_estado_mensaje(resultado, estado, mensaje)
columnas_necesarias_stops = ['stop_id','stop_code','stop_name','stop_desc','stop_lat','stop_lon','zone_id','stop_url','location_type','parent_station','wheelchair_boarding']
df_stops = verificar_columnas(df_stops, columnas_necesarias_stops, 'stops.txt')

#Leemos el archivo trips.txt , lo guardamos en un dataframe y actualizamos la BD
try:
    df_trips = pd.read_csv('txts/trips.txt', dtype=str)
except Exception as e:
        estado = "Error"
        mensaje = "Se presentaron problemas al momento de transformar el archivo trips.txt a dataframe"
        actualizar_estado_mensaje(resultado, estado, mensaje)
columnas_necesarias_trips = ['route_id','service_id','trip_id','trip_headsign','trip_short_name','direction_id','block_id','shape_id','wheelchair_accessible','bikes_allowed']
df_trips = verificar_columnas(df_trips, columnas_necesarias_trips, 'trips.txt')



### Aqui es donde se produce la actualizacion de los registros en la base de datos.

#Se comprueba si el la base deatos esta poblada o no invocando a la funcion especial para este proceso.
#Si es True que la base datos esta vacia y se pobla con los archivos entrantes
if verificar_base_de_datos_vacia(session):

    cargar_datos(df_agency, 'Agency', engine)
    cargar_datos(df_routes, 'Route', engine)
    cargar_datos(df_calendar, 'Calendar', engine)
    cargar_datos(df_stops, 'Stop', engine)
    cargar_datos(df_trips, 'Trip', engine)
    cargar_datos(df_stop_times, 'Stop_Times', engine)
    
else:
    # Se generan dataframes por cada tabla de los archivos GTFS
    with engine.connect() as connection:
        df_db_agency = pd.read_sql_table('Agency', con=connection)
        df_db_routes = pd.read_sql_table('Route', con=connection)
        df_db_calendar = pd.read_sql_table('Calendar', con=connection)
        df_db_stops = pd.read_sql_table('Stop', con=connection)
        df_db_trips = pd.read_sql_table('Trip', con=connection)
        df_db_stop_times = pd.read_sql_table('Stop_Times', con=connection)

    # Se actualizan los registros, eliminando los registros obsolestos en la base de datos
    eliminar_registros_antiguos(session, df_db_stop_times, df_db_stop_times, Stop_Times,'Stop_Times')
    eliminar_registros_antiguos(session, df_db_trips, df_trips, Trip,'Trip')
    eliminar_registros_antiguos(session, df_db_routes, df_routes, Route,'Route')
    eliminar_registros_antiguos(session, df_db_agency, df_agency, Agency,'Agency')
    eliminar_registros_antiguos(session, df_db_calendar, df_calendar, Calendar,'Calendar')
    eliminar_registros_antiguos(session, df_db_stops, df_stops, Stop, 'Stop')

    # Se actualizan los registros, agrefando registros nuevos a la base de datos  
    agregar_registros_nuevos(df_db_agency, df_agency, 'Agency', engine)
    agregar_registros_nuevos(df_db_routes, df_routes, 'Route', engine)
    agregar_registros_nuevos(df_db_calendar, df_calendar, 'Calendar', engine)
    agregar_registros_nuevos(df_db_stops, df_stops, 'Stop', engine)
    agregar_registros_nuevos(df_db_trips, df_trips, 'Trip', engine)
    agregar_registros_nuevos(df_db_stop_times, df_stop_times, 'Stop_Times', engine)


# Creo un nuevo archivo ZIP filtrado con los datos esperados/necesarios y generar un nuevo estandar de archivos gtfs
shutil.make_archive('zips_procesados/'+nombre_zip+'_procesado','zip','txts')
estado = "Procesando"
mensaje = "Nuevo archivo ZIP generado, contiene unicamente archivos esperados"
actualizar_estado_mensaje(resultado,estado,mensaje)