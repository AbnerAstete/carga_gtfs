from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.declarative import DeferredReflection

# Configuración de la base de datos
DATABASE_URL = "postgresql://postgres:5314806Jair@localhost:5432/fastApi_gtfs"
engine = create_engine(DATABASE_URL)

# Crea una clase base para tus modelos utilizando DeferredReflection
Base = declarative_base(cls=DeferredReflection)
Base.metadata.bind = engine

