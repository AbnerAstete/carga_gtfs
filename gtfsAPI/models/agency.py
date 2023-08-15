from sqlalchemy import Table,DateTime, ForeignKey, Column, String, Integer, func, Boolean, Float
from database.db import Base
from sqlalchemy.orm import relationship

class Agency(Base):
    __tablename__ = 'agency'

    id_carga_gtfs = Column(Integer, ForeignKey('carga_gtfs.id_carga_gtfs'), nullable=True)
    agency_id = Column(String(255), primary_key=True)
    agency_name = Column(String(255))
    agency_url = Column(String(255))
    agency_timezone = Column(String(50))
    agency_lang = Column(String(10))
    agency_phone = Column(String(50))
    agency_fare_url = Column(String(255))

    carga_gtfs = relationship("CargaGtfs", back_populates="agency")

    def __repr__(self):
        return f"{self.agency_name}"