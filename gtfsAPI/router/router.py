from fastapi import APIRouter

from database.db import Base,engine

from models import agency,calendar,carga_gtfs,route,shape_refence,shapes,stop_times,stops,trip

Base.metadata.create_all(bind=engine)


create_gtfs = APIRouter()


@create_gtfs.get("/")
def root():
    return "Hola"
