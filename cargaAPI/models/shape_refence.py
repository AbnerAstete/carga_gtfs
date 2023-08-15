from sqlalchemy import ForeignKey, Column, String, Integer
from database.db import Base
from sqlalchemy.orm import relationship

class ShapeReference(Base):
    __tablename__ = 'shape_reference'

    id_carga_gtfs = Column(Integer, ForeignKey('carga_gtfs.id_carga_gtfs'), nullable=False)
    shape_id = Column(String(50), primary_key=True)

    def __repr__(self):
        return f"Shape ID: {self.shape_id}"