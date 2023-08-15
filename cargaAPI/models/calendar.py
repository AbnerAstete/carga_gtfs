from sqlalchemy import ForeignKey, Column, String, Integer
from database.db import Base
from sqlalchemy.orm import relationship

class Calendar(Base):
    __tablename__ = 'calendar'

    id_carga_gtfs = Column(Integer, ForeignKey('carga_gtfs.id_carga_gtfs'), nullable=False)
    service_id = Column(String(50), primary_key=True)
    start_date = Column(String(50))
    end_date = Column(String(50))
    monday = Column(String)
    tuesday = Column(String)
    wednesday = Column(String)
    thursday = Column(String)
    friday = Column(String)
    saturday = Column(String)
    sunday = Column(String)

    def __repr__(self):
        return self.service_id