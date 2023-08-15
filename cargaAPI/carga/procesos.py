import os 
from os import remove
import zipfile

import pandas as pd
import shutil


from database.db import engine

from sqlalchemy import desc,func,case,text
from sqlalchemy.orm import Session
from sqlalchemy.sql import exists

from models.carga_gtfs import CargaGtfs
from models.agency import Agency
from models.calendar import Calendar
from models.route import Route
from models.shape_refence import ShapeReference
from models.shapes import Shapes
from models.stop_times import StopTimes
from models.stops import Stop
from models.trip import Trip





def actualizar_estado(id_carga_gtfs:int, estado:str, mensaje:str, db: Session):
    zip = db.query(CargaGtfs).filter(CargaGtfs.id_carga_gtfs == id_carga_gtfs).first()
    zip.estado = estado
    zip.mensaje = mensaje
    db.commit()

def verificar_columnas(dataframe, columnas_necesarias, archivo, zip, db):
    columnas_sobrantes = [columna for columna in dataframe.columns if columna not in columnas_necesarias]
    columnas_faltantes = [columna for columna in columnas_necesarias if columna not in dataframe.columns]

    dataframe_filtrado = dataframe.drop(columnas_sobrantes, axis=1)
    
    if len(columnas_faltantes) > 0:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje=f"Columnas faltantes en archivo {archivo}, por favor asegúrese de que el archivo contenga las siguientes columnas: {', '.join(columnas_faltantes)}", db=db)
        raise ValueError(f"Columnas faltantes en archivo {archivo}, por favor asegúrese de que el archivo contenga las siguientes columnas: {', '.join(columnas_faltantes)}")
    else:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje=f"Archivo '{archivo}' leído y comprobado que contenga todas las columnas necesarias, siguiendo proceso...", db=db)
        return dataframe_filtrado

def cargar_datos(df,zip,tabla,db,engine):
    try:
        df.to_sql(tabla, engine, if_exists='append', index=False)
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje=f"Archivo '{tabla}.txt' ha sido cargado a la tabla, siguiendo proceso...", db=db)

    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje=f"Se presentaron problemas con el archivo '{tabla}.txt' al momento de ingresar los datos a la tabla.", db=db)







def extraer_zip_futuro(zip,zip_ruta,db):
    with zipfile.ZipFile(zip_ruta, 'r') as archivo_zip:
        archivo_zip.extractall('txts/') 
    actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje="Extrayendo archivos del último ZIP ingresado.", db=db)

def extraer_zip(zip,zip_ruta,db):
    with zipfile.ZipFile(zip_ruta, 'r') as archivo_zip:
            archivo_zip.extractall('txts/') 
    actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje="Extrayendo archivos del último ZIP ingresado.", db=db)



def identificar_zip(db: Session):
    zip = db.query(CargaGtfs).order_by(desc('fecha')).first()
    return zip

def inspeccionar_txts(zip,almacen_txts:str,zip_ruta:str,db: Session):
    archivos_en_txt = os.listdir(almacen_txts)
    archivos_esperados = ['agency.txt', 'calendar.txt', 'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt', 'shapes.txt']
    archivos_faltantes = [archivo for archivo in archivos_esperados if archivo not in archivos_en_txt]

    if len(archivos_faltantes) > 0:

        for archivo in archivos_en_txt:
            archivo_ruta = os.path.join(almacen_txts, archivo)
            os.remove(archivo_ruta)
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje="Archivos Faltantes, por favor ingrese los siguientes archivos: "+",".join(archivos_faltantes), db=db)

        remove(zip_ruta)
        raise ValueError("Archivos Faltantes, por favor ingrese los siguientes archivos: "+",".join(archivos_faltantes))
    
    for archivo in archivos_en_txt:
        if archivo not in archivos_esperados:
            os.remove(almacen_txts+archivo)

    actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje="Total de archivos esperados, siguiendo proceso...", db=db)


def transformar_txts(zip,db: Session):

    id_carga_gtfs = zip.id_carga_gtfs
    
    try:
        df_agency = pd.read_csv('txts/agency.txt', dtype=str)
        columnas_necesarias_agency = ['agency_id','agency_name','agency_url','agency_timezone','agency_lang','agency_phone','agency_fare_url']
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje="Se presentaron problemas al momento de transformar el archivo agency.txt a dataframe", db=db)
    df_agency = verificar_columnas(df_agency, columnas_necesarias_agency, 'agency.txt', zip, db)
    df_agency['id_carga_gtfs'] = id_carga_gtfs
    
    try:
        df_calendar = pd.read_csv('txts/calendar.txt', dtype=str)
        columnas_necesarias_calendar = ['service_id','start_date','end_date','monday','tuesday','wednesday','thursday','friday','saturday','sunday']
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje="Se presentaron problemas al momento de transformar el archivo calendar.txt a dataframe", db=db)
    df_calendar = verificar_columnas(df_calendar, columnas_necesarias_calendar, 'calendar.txt', zip, db)
    df_calendar['id_carga_gtfs'] = id_carga_gtfs

    try:
        df_routes = pd.read_csv('txts/routes.txt', dtype=str)
        columnas_necesarias_route = ['route_id','agency_id','route_short_name','route_long_name','route_desc','route_type','route_url','route_color','route_text_color']
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje="Se presentaron problemas al momento de transformar el archivo routes.txt a dataframe", db=db)
    df_routes = verificar_columnas(df_routes, columnas_necesarias_route, 'routes.txt', zip, db)
    df_routes['id_carga_gtfs'] = id_carga_gtfs
    #Por este caso que leemos todos los campos como str hay que cambiar la id de route a int para que tenga el mismo tipo de dato que la db
    df_routes['route_id'] = df_routes['route_id'].astype(int)

    try:
        df_stop_times = pd.read_csv('txts/stop_times.txt', dtype=str)
        columnas_necesarias_stop_times = ['trip_id','arrival_time','departure_time','stop_id','stop_sequence','stop_headsign','pickup_type','drop_off_type','timepoint']
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje="Se presentaron problemas al momento de transformar el archivo stop_times.txt a dataframe", db=db)
    df_stop_times = verificar_columnas(df_stop_times, columnas_necesarias_stop_times, 'stop_times.txt', zip, db)
    df_stop_times['id_carga_gtfs'] = id_carga_gtfs

    try:
        df_stops = pd.read_csv('txts/stops.txt', dtype=str)
        columnas_necesarias_stops = ['stop_id','stop_code','stop_name','stop_desc','stop_lat','stop_lon','zone_id','stop_url','location_type','parent_station','wheelchair_boarding']
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje="Se presentaron problemas al momento de transformar el archivo stops.txt a dataframe", db=db)
    df_stops = verificar_columnas(df_stops, columnas_necesarias_stops, 'stops.txt', zip, db)
    df_stops['id_carga_gtfs'] = id_carga_gtfs
    df_stops['stop_id_interno'] = range(1, len(df_stops) + 1)

    try:
        df_trips = pd.read_csv('txts/trips.txt', dtype=str)
        columnas_necesarias_trips = ['route_id','service_id','trip_id','trip_headsign','trip_short_name','direction_id','block_id','shape_id','wheelchair_accessible','bikes_allowed']
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje="Se presentaron problemas al momento de transformar el archivo trips.txt a dataframe", db=db)
    df_trips = verificar_columnas(df_trips, columnas_necesarias_trips, 'trips.txt', zip, db)
    df_trips['id_carga_gtfs'] = id_carga_gtfs
    df_trips.rename(columns={'shape_id': 'shape_reference_id'}, inplace=True)
    #Por este caso que leemos todos los campos como str hay que cambiar la id de route a int para que tenga el mismo tipo de dato que la db
    df_trips['route_id'] = df_trips['route_id'].astype(int)

    try:
        df_shapes = pd.read_csv('txts/shapes.txt', dtype=str)
        columnas_necesarias_shapes= ['shape_id','shape_pt_sequence','shape_pt_lat','shape_pt_lon']
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje="Se presentaron problemas al momento de transformar el archivo trips.txt a dataframe", db=db)
    df_shapes = verificar_columnas(df_shapes, columnas_necesarias_shapes, 'shapes.txt', zip, db)
    df_shapes['id_carga_gtfs'] = id_carga_gtfs

    # Se genera el dataframe correspondiente a la tabla shapes_reference.
    df_shapes_reference = df_shapes[['shape_id']].drop_duplicates().reset_index(drop=True)
    df_shapes_reference['id_carga_gtfs'] = id_carga_gtfs


    return df_agency,df_calendar,df_routes,df_stop_times,df_stops,df_trips,df_shapes,df_shapes_reference

def verificar_registros_bd(db: Session):
    tablas = [Agency,Calendar,Route,ShapeReference,Shapes,StopTimes,Stop,Trip]
    for tabla in tablas:
        tiene_registros = db.query(exists().where(tabla.id_carga_gtfs.isnot(None))).scalar()
        if tiene_registros:
            return False  # Si al menos una tabla tiene registros, la base de datos no está vacía
    return True #Si ninguna tabla tiene registros, la base de datos está vacía

def primera_carga(zip,db: Session,engine,df_agency,df_calendar,df_routes,df_stop_times,df_stops,df_trips,df_shapes,df_shapes_reference):
    cargar_datos(df_agency,zip,'agency',db,engine)
    cargar_datos(df_routes,zip,'route',db,engine)
    cargar_datos(df_calendar,zip,'calendar',db,engine)
    cargar_datos(df_stops,zip,'stop',db,engine)
    cargar_datos(df_shapes_reference,zip,'shape_reference',db,engine)
    cargar_datos(df_shapes,zip,'shapes', db,engine)
    cargar_datos(df_trips,zip,'trip',db,engine)
    cargar_datos(df_stop_times,zip,'stop_times', db,engine)

def extraer_db(engine):
    with engine.connect() as connection:
        df_db_agency = pd.read_sql_table('agency', con=connection)
        df_db_routes = pd.read_sql_table('route', con=connection)
        df_db_calendar = pd.read_sql_table('calendar', con=connection)
        df_db_stops = pd.read_sql_table('stop', con=connection)
        df_db_shapes_reference = pd.read_sql_table('shape_reference', con=connection)
        df_db_shapes = pd.read_sql_table('shapes', con=connection)
        df_db_trips = pd.read_sql_table('trip', con=connection)
        df_db_stop_times = pd.read_sql_table('stop_times', con=connection)
    
    return df_db_agency,df_db_routes,df_db_calendar,df_db_stops,df_db_shapes_reference,df_db_shapes,df_db_trips,df_db_stop_times


def eliminar_registros_antiguos(session, df_db, df,modelo,tabla):

        comparar = df_db.merge(df, indicator=True, how='outer')
        registros_antiguos = comparar.loc[lambda x: x['_merge'] == 'left_only'].drop(columns='_merge')
        for i, row in registros_antiguos.iterrows():
            session.query(modelo).filter_by(**{modelo.__table__.primary_key.columns.keys()[0]: row[0]}).delete()
            session.commit()


def eliminar_registros(zip,db,df_db,df,id_tabla,tabla):
    df_db_sin_id = df_db.drop(columns='id_carga_gtfs')
    df_sin_id = df.drop(columns='id_carga_gtfs')

    comparar = df_db_sin_id.merge(df_sin_id, indicator=True, how='outer')
    registros_antiguos = comparar.loc[lambda x: x['_merge'] == 'left_only']

    registros_antiguos['id_carga_gtfs'] = df_db.loc[registros_antiguos.index, 'id_carga_gtfs']
    registros_antiguos = registros_antiguos.drop(columns='_merge')
    try:
        registros_a_eliminar = registros_antiguos[id_tabla].tolist()

        if registros_a_eliminar:
            query = text(f"DELETE FROM {tabla} WHERE {id_tabla} IN ({', '.join(map(repr, registros_a_eliminar))})")
            db.execute(query)
            actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje=f"Se eliminaron los registros obsoletos, no se encontraban en el dataframe '{tabla}' entrante.", db=db)
        else:
            actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje=f"No se encontraron registros obsoletos para eliminar en la tabla '{tabla}'.", db=db)

    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje=f"Se presentaron problemas con el al momento de eliminar los registros que no se encontraban en el'{tabla}' entrante.", db=db)
    return registros_antiguos


def eliminar_registros_stop_time(zip, db, df_db, df, pk1, pk2, tabla):
    df_db_sin_id = df_db.drop(columns='id_carga_gtfs')
    df_sin_id = df.drop(columns='id_carga_gtfs')

    comparar = df_db_sin_id.merge(df_sin_id, indicator=True, how='outer')
    registros_antiguos = comparar.loc[lambda x: x['_merge'] == 'left_only']

    registros_antiguos['id_carga_gtfs'] = df_db.loc[registros_antiguos.index, 'id_carga_gtfs']
    registros_antiguos = registros_antiguos.drop(columns='_merge')
    registros_a_eliminar = registros_antiguos[[pk1, pk2]].values.tolist()
    
    try:
        registros_a_eliminar_str = ', '.join(f"('{r[0]}', '{r[1]}')" for r in registros_a_eliminar)
        if registros_a_eliminar:
            query = text(f"DELETE FROM {tabla} WHERE ({pk1}, {pk2}) IN ({registros_a_eliminar_str})")
            print(query)
            db.execute(query, {"registros_a_eliminar": registros_a_eliminar})
            actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje=f"Se eliminaron los registros obsoletos de la tabla '{tabla}'.", db=db)
        else:
            actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje=f"No se encontraron registros obsoletos para eliminar en la tabla '{tabla}'.", db=db)

    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje=f"Hubo un error al eliminar los registros obsoletos de la tabla '{tabla}'.", db=db)

def agregar_registros(zip,db,df_db,df,tabla):
    df_db_sin_id = df_db.drop(columns='id_carga_gtfs')
    df_sin_id = df.drop(columns='id_carga_gtfs')
    comparar = df_db_sin_id.merge(df_sin_id, indicator=True, how='outer')
    
    registros_nuevos = comparar.loc[lambda x: x['_merge'] == 'right_only']

    registros_nuevos['id_carga_gtfs'] = df.loc[registros_nuevos.index]['id_carga_gtfs']
    registros_nuevos = registros_nuevos.drop(columns='_merge')

    try:
        registros_nuevos.to_sql(tabla, engine, if_exists='append', index=False)
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje=f"Se agregaron los registros nuevos en la tabla de '{tabla}'.", db=db)
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje=f"Se presentaron problemas al momento de agregar registros nuevos en la tabla de '{tabla}'.", db=db)
    
    return registros_nuevos


def agregar_registros_stops(zip,db,df_db,df,tabla):
    df_db_sin_id = df_db.drop(columns=['id_carga_gtfs', 'stop_id_interno'])
    df_sin_id = df.drop(columns='id_carga_gtfs')

    comparar = df_db_sin_id.merge(df_sin_id, on=list(df_db_sin_id.columns), indicator=True, how='outer')
    registros_nuevos = comparar.loc[lambda x: x['_merge'] == 'right_only']
    registros_nuevos['id_carga_gtfs'] = df.loc[registros_nuevos.index]['id_carga_gtfs']
    registros_nuevos = registros_nuevos.drop(columns='_merge')

    max_existing_id = df_db['stop_id_interno'].max()
    registros_nuevos['stop_id_interno'] = range(max_existing_id + 1, max_existing_id + 1 + len(registros_nuevos))

    try:
        registros_nuevos.to_sql(tabla, engine, if_exists='append', index=False)
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Procesando", mensaje=f"Se agregaron los registros nuevos en la tabla de '{tabla}'.", db=db)
    except Exception as e:
        actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Error", mensaje=f"Se presentaron problemas al momento de agregar registros nuevos en la tabla de '{tabla}'.", db=db)



def verificar_registros_antiguos(zip,db: Session,df_agency,df_calendar,df_routes,df_stop_times,df_stops,df_trips,df_shapes,df_shapes_reference,df_db_agency,df_db_routes,df_db_calendar,df_db_stops,df_db_shapes_reference,df_db_shapes,df_db_trips,df_db_stop_times):
    #eliminar_registros_stop_time(zip,db,df_db_stop_times,df_stop_times,'trip_id','stop_id','stop_times')
    # eliminar_registros(zip,db,df_db_trips,df_trips,'trip_id','trip')
    # eliminar_registros(zip,db,df_db_shapes,df_shapes,'shape_id','shapes')
    # eliminar_registros(zip,db,df_db_shapes_reference,df_shapes_reference,'shape_id','shape_reference')    
    # eliminar_registros(zip,db,df_db_calendar,df_calendar,'service_id','calendar')
    # eliminar_registros(zip,db,df_db_routes,df_routes,'route_id','route')   
    df_agency_antiguos_registros = eliminar_registros(zip,db,df_db_agency,df_agency,'agency_id','agency')
    # eliminar_registros(zip,db,df_db_stops,df_stops,'stop_id','stop')
    return df_agency_antiguos_registros

def verificar_registros_nuevos(zip,db: Session,df_agency,df_calendar,df_routes,df_stop_times,df_stops,df_trips,df_shapes,df_shapes_reference,df_db_agency,df_db_routes,df_db_calendar,df_db_stops,df_db_shapes_reference,df_db_shapes,df_db_trips,df_db_stop_times):
    df_agency_nuevos_registros = agregar_registros(zip,db,df_db_agency,df_agency,'agency')
    # agregar_registros(zip,db,df_db_routes,df_routes,'route')
    # agregar_registros(zip,db,df_db_calendar,df_calendar,'calendar')
    # agregar_registros(zip,db,df_db_shapes_reference,df_shapes_reference,'shape_reference')
    # agregar_registros(zip,db,df_db_shapes,df_shapes,'shapes')
    # agregar_registros(zip,db,df_db_trips,df_trips,'trip')
    # agregar_registros_stops(zip,db,df_db_stops,df_stops,'stop')
    # agregar_registros(zip,db,df_db_stop_times,df_stop_times,'stop_times')
    return df_agency_nuevos_registros


def terminar_carga(zip,db,zips_procesados):
    shutil.make_archive('zips_procesados/'+zip.zip+'_procesado','zip','txts')
    actualizar_estado(id_carga_gtfs=zip.id_carga_gtfs, estado="Terminado", mensaje='Nuevo archivo ZIP generado, contiene unicamente archivos esperados', db=db)