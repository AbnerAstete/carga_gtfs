from sqlalchemy import ForeignKey, Column, String, Integer
from database.db import Base
from sqlalchemy.orm import relationship

class Stop(Base):
    __tablename__ = 'stop'

    id_carga_gtfs = Column(Integer, ForeignKey('carga_gtfs.id_carga_gtfs'), nullable=False)
    stop_id_interno = Column(Integer)  
    stop_id = Column(String(50), primary_key=True)  
    stop_code = Column(String(50))
    stop_name = Column(String(255))
    stop_desc = Column(String(255))
    stop_lat = Column(String(50))
    stop_lon = Column(String(50))
    zone_id = Column(String(50))
    stop_url = Column(String(255))
    location_type = Column(String(50))
    parent_station = Column(String(50))
    wheelchair_boarding = Column(String(50))

    def __repr__(self):
        return self.stop_name
