from sqlalchemy import ForeignKey, Column, String, Integer
from database.db import Base
from sqlalchemy.orm import relationship

class Shapes(Base):
    __tablename__ = 'shapes'

    id_carga_gtfs = Column(Integer, ForeignKey('carga_gtfs.id_carga_gtfs'), nullable=False)
    shape_id = Column(String(50), ForeignKey('shape_reference.shape_id'), primary_key=True)
    shape_pt_lat = Column(String)
    shape_pt_lon = Column(String)
    shape_pt_sequence = Column(String, primary_key=True)

    carga_gtfs = relationship("CargaGtfs", back_populates="shapes")
    shape_reference = relationship("ShapeReference", back_populates="shapes")

    def __repr__(self):
        return f"Shape ID: {self.shape_id}, Lat: {self.shape_pt_lat}, Lon: {self.shape_pt_lon}"