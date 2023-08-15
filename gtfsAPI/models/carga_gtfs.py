from sqlalchemy import DateTime, Column, String, Integer, func
from database.db import Base

class CargaGtfs(Base):
    __tablename__ = 'carga_gtfs'

    id_carga_gtfs = Column(Integer, primary_key=True,autoincrement=True)
    id_usuario = Column(String(255))
    zip = Column(String(255))
    fecha = Column(DateTime, server_default=func.now())
    estado = Column(String(50))
    mensaje = Column(String(255))

    def __repr__(self):
        return f"{self.id_usuario}, {self.zip}, {self.fecha}, {self.estado}, {self.mensaje}"