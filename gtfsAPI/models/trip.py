from sqlalchemy import Table,DateTime, ForeignKey, Column, String, Integer, func, Boolean, Float
from database.db import Base
from sqlalchemy.orm import relationship

class Trip(Base):
    __tablename__ = 'trip'

    id_carga_gtfs = Column(Integer, ForeignKey('carga_gtfs.id_carga_gtfs'), nullable=False)
    route_id = Column(Integer, ForeignKey('route.route_id'), nullable=False)
    service_id = Column(String(50), ForeignKey('calendar.service_id'), nullable=False)
    shape_reference_id = Column(String(50), ForeignKey('shape_reference.shape_id'), nullable=False)
    trip_id = Column(String(50), primary_key=True)
    trip_headsign = Column(String(255))
    trip_short_name = Column(String(255))
    direction_id = Column(String(50))
    block_id = Column(String(50))
    wheelchair_accessible = Column(String(50))
    bikes_allowed = Column(String(50))

    carga_gtfs = relationship("CargaGtfs", back_populates="trips")
    route = relationship("Route", back_populates="trips")
    service = relationship("Calendar", back_populates="trips")
    shape_reference = relationship("ShapeReference", back_populates="trips")

    def __repr__(self):
        return f"Trip ID: {self.trip_id}, Route ID: {self.route_id}, Service ID: {self.service_id}"