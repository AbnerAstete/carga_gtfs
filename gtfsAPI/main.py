from fastapi import FastAPI
from router.router import create_gtfs

app = FastAPI()

app.include_router(create_gtfs)
