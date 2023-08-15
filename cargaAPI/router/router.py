from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import sessionmaker,Session
from database.db import engine,Base
from carga.procesos import extraer_zip_futuro,extraer_zip,identificar_zip,inspeccionar_txts,transformar_txts,verificar_registros_bd,primera_carga,extraer_db,verificar_registros_antiguos,verificar_registros_nuevos,terminar_carga

Base.prepare(engine)

def get_db():
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

almacen_zips = 'zip/'
almacen_txts =  'txts/'
zips_procesados = 'zips_procesados/'


carga_gtfs = APIRouter()


@carga_gtfs.get("/")
def root():
    return "Pagina Principal. Use /docs en la url para probrar la carga."

@carga_gtfs.post("/carga")
async def carga(db: Session = Depends(get_db)):

    zip_info = identificar_zip(db=db)
    zip_ruta = almacen_zips + zip_info.zip + ".zip"
 
    extraer_zip(zip=zip_info,zip_ruta=zip_ruta,db=db)  
    inspeccionar_txts(zip=zip_info,almacen_txts=almacen_txts,zip_ruta=zip_ruta,db=db)
    df_agency, df_calendar, df_routes,df_stop_times, df_stops, df_trips, df_shapes, df_shapes_reference = transformar_txts(zip=zip_info, db=db)
    bd_vacia = verificar_registros_bd(db=db)
    if bd_vacia:
            primera_carga(zip=zip_info,
                      engine=engine,
                      df_agency=df_agency,
                      df_calendar=df_calendar,
                      df_routes=df_routes,
                      df_stop_times=df_stop_times,
                      df_stops=df_stops,
                      df_trips=df_trips,
                      df_shapes=df_shapes,
                      df_shapes_reference=df_shapes_reference,
                      db=db)
    else:
        df_db_agency,df_db_routes,df_db_calendar,df_db_stops,df_db_shapes_reference,df_db_shapes,df_db_trips,df_db_stop_times = extraer_db(engine=engine)

        verificar_registros_antiguos(zip=zip_info,
                      df_agency=df_agency,
                      df_calendar=df_calendar,
                      df_routes=df_routes,
                      df_stop_times=df_stop_times,
                      df_stops=df_stops,
                      df_trips=df_trips,
                      df_shapes=df_shapes,
                      df_shapes_reference=df_shapes_reference,
                      df_db_agency=df_db_agency,
                      df_db_routes=df_db_routes,
                      df_db_calendar=df_db_calendar,
                      df_db_stops=df_db_stops,
                      df_db_shapes_reference=df_db_shapes_reference,
                      df_db_shapes=df_db_shapes,
                      df_db_trips=df_db_trips,
                      df_db_stop_times=df_db_stop_times,  
                      db=db)
        
        verificar_registros_nuevos(zip=zip_info,
                      df_agency=df_agency,
                      df_calendar=df_calendar,
                      df_routes=df_routes,
                      df_stop_times=df_stop_times,
                      df_stops=df_stops,
                      df_trips=df_trips,
                      df_shapes=df_shapes,
                      df_shapes_reference=df_shapes_reference,
                      df_db_agency=df_db_agency,
                      df_db_routes=df_db_routes,
                      df_db_calendar=df_db_calendar,
                      df_db_stops=df_db_stops,
                      df_db_shapes_reference=df_db_shapes_reference,
                      df_db_shapes=df_db_shapes,
                      df_db_trips=df_db_trips,
                      df_db_stop_times=df_db_stop_times,  
                      db=db)
    
    terminar_carga(zip=zip_info,zips_procesados=zips_procesados,db=db)

    return "Proceso Finalizado"