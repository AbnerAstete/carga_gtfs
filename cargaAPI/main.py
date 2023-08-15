from fastapi import FastAPI
from router.router import carga_gtfs

app = FastAPI()

app.include_router(carga_gtfs)
