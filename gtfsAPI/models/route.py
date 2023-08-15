from sqlalchemy import ForeignKey, Column, String, Integer
from database.db import Base
from sqlalchemy.orm import relationship


class Route(Base):
    __tablename__ = 'route'

    id_carga_gtfs = Column(Integer, ForeignKey('carga_gtfs.id_carga_gtfs'), nullable=False)
    route_id = Column(Integer, primary_key=True)
    agency_id = Column(String(255), ForeignKey('agency.agency_id'), nullable=False)
    route_short_name = Column(String(255))
    route_long_name = Column(String(255))
    route_desc = Column(String(255))
    route_type = Column(String(50))
    route_url = Column(String(255))
    route_color = Column(String(50))
    route_text_color = Column(String(50))

    carga_gtfs = relationship("CargaGtfs", back_populates="routes")
    agency = relationship("Agency", back_populates="routes")

    def __repr__(self):
        return self.route_short_name
