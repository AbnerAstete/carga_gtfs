from sqlalchemy import Table,DateTime, ForeignKey, Column, String, Integer, func, Boolean, Float
from database.db import Base
from sqlalchemy.orm import relationship

class StopTimes(Base):
    __tablename__ = 'stop_times'

    id_carga_gtfs = Column(Integer, ForeignKey('carga_gtfs.id_carga_gtfs'), nullable=False)
    trip_id = Column(String(50), ForeignKey('trip.trip_id'), nullable=False, primary_key=True)
    arrival_time = Column(String(50))
    departure_time = Column(String(50))
    stop_id = Column(String(50), ForeignKey('stop.stop_id'), nullable=False, primary_key=True)
    stop_sequence = Column(String(50))
    stop_headsign = Column(String(255))
    pickup_type = Column(String(50))
    drop_off_type = Column(String(50))
    timepoint = Column(String(50))

    def __repr__(self):
        return f"Trip ID: {self.trip_id}, Stop ID: {self.stop_id}"
    