from sqlalchemy import DateTime, create_engine, ForeignKey, Column, String, Integer, CHAR, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Zip(Base):
    __tablename__ = "carga_zip"

    id_transaccion = Column("id_transaccion",Integer, primary_key=True, autoincrement=True)
    id_usuario = Column("id_usuario", Integer)
    usuario = Column("usuario",String)
    nombre_zip = Column("nombre_zip", String)
    fecha = Column(DateTime(timezone=True), server_default=func.now())
    estado = Column("estado", String)
    mensaje = Column("mensaje", String)

    def __init__(self,id_usuario,usuario,nombre_zip,estado,mensaje):
        self.id_usuario = id_usuario
        self.usuario = usuario
        self.nombre_zip = nombre_zip
        self.estado = estado
        self.mensaje = mensaje
    
    def __repr__(self):
        return f"{self.id_usuario}, {self.usuario}, {self.nombre_zip}, {self.estado}, {self.mensaje}"

class Agency(Base):
    __tablename__ = 'Agency'

    agency_id = Column("agency_id",String, primary_key=True)
    agency_name = Column("agency_name",String)
    agency_url = Column("agency_url",String)
    agency_timezone = Column("agency_timezone",String)
    agency_lang = Column("agency_lang",String)
    agency_phone = Column("agency_phone",String)
    agency_fare_url = Column("agency_fare_url",String)


    def __init__(self,agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url):
        self.agency_id = agency_id
        self.agency_name = agency_name
        self.agency_url = agency_url
        self.agency_timezone = agency_timezone
        self.agency_lang = agency_lang
        self.agency_phone = agency_phone
        self.agency_fare_url = agency_fare_url

class Route(Base):
    __tablename__ = 'Route'

    route_id = Column("route_id",String, primary_key=True)
    #Le agregue la FK de la PK de Agency
    agency_id = Column("agency_id",String,ForeignKey('Agency.agency_id'))
    route_short_name = Column("route_short_name",String)
    route_long_name = Column("route_long_name",String)
    route_desc = Column("route_desc",String)
    route_type = Column("route_type",String)
    route_url = Column("route_url",String)
    route_color = Column("route_color",String)
    route_text_color = Column("route_text_color",String)

    def __init__(self,route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color):
        self.route_id = route_id
        self.agency_id = agency_id
        self.route_short_name = route_short_name
        self.route_long_name = route_long_name
        self.route_desc = route_desc
        self.route_type = route_type
        self.route_url = route_url
        self.route_color = route_color
        self.route_text_color = route_text_color

class Calendar(Base):
    __tablename__ = 'Calendar'

    service_id = Column("service_id",String, primary_key=True)
    start_date = Column("start_date",String)
    end_date = Column("end_date",String)
    monday = Column("monday",String)
    tuesday = Column("tuesday",String)
    wednesday = Column("wednesday",String)
    thursday = Column("thursday",String)
    friday = Column("friday",String)
    saturday = Column("saturday",String)
    sunday = Column("sunday",String)

    def __init__(self,service_id,start_date,end_date,monday,tuesday,wednesday,thursday,friday,saturday,sunday):
        self.service_id = service_id
        self.start_date = start_date
        self.end_date = end_date
        self.monday = monday
        self.tuesday = tuesday
        self.wednesday = wednesday
        self.thursday = thursday
        self.friday = friday
        self.saturday = saturday
        self.sunday = sunday

class Stop(Base):
    __tablename__ = 'Stop'

    stop_id = Column("stop_id",String, primary_key=True)
    stop_code = Column("stop_code",String)
    stop_name = Column("stop_name",String)
    stop_desc = Column("stop_desc",String)
    stop_lat = Column("stop_lat",String)
    stop_lon = Column("stop_lon",String)
    zone_id = Column("zone_id",String)
    stop_url = Column("stop_url",String)
    location_type = Column("location_type",String)
    parent_station = Column("parent_station",String)
    wheelchair_boarding = Column("wheelchair_boarding",String)

    def __init__(self,stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,wheelchair_boarding):
        self.stop_id = stop_id
        self.stop_code = stop_code
        self.stop_name = stop_name
        self.stop_desc = stop_desc
        self.stop_lat = stop_lat
        self.stop_lon = stop_lon
        self.zone_id = zone_id
        self.stop_url = stop_url
        self.location_type = location_type
        self.parent_station = parent_station
        self.wheelchair_boarding = wheelchair_boarding

class Trip(Base):
    __tablename__ = 'Trip'

    route_id = Column("route_id",String, ForeignKey('Route.route_id'))
    service_id = Column("service_id",String, ForeignKey('Calendar.service_id'))
    trip_id = Column("trip_id",String, primary_key=True)
    trip_headsign = Column("trip_headsign",String)
    trip_short_name = Column("trip_short_name",String)
    direction_id = Column("direction_id",String)
    block_id = Column("block_id",String)
    shape_id = Column("shape_id",String)
    wheelchair_accessible = Column("wheelchair_accessible",String)
    bikes_allowed = Column("bikes_allowed",String)

    def __init__(self,route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,bikes_allowed):
        self.route_id = route_id
        self.service_id = service_id
        self.trip_id = trip_id
        self.trip_headsign = trip_headsign
        self.trip_short_name = trip_short_name
        self.direction_id = direction_id
        self.block_id = block_id
        self.shape_id = shape_id
        self.wheelchair_accessible = wheelchair_accessible
        self.bikes_allowed = bikes_allowed

class Stop_Times(Base):
    __tablename__ = 'Stop_Times'

    trip_id = Column("trip_id",String,ForeignKey(Trip.trip_id), primary_key=True)
    arrival_time = Column("arrival_time",String)
    departure_time = Column("departure_time",String)
    stop_id = Column("stop_id",String,ForeignKey('Stop.stop_id'), primary_key=True)
    stop_sequence = Column("stop_sequence",String)
    stop_headsign = Column("stop_headsign",String)
    pickup_type = Column("pickup_type",String)
    drop_off_type = Column("drop_off_type",String)
    timepoint = Column("timepoint",String)


    def __init__(self,trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,timepoint):
        self.trip_id = trip_id
        self.arrival_time = arrival_time
        self.departure_time = departure_time
        self.stop_id = stop_id
        self.stop_sequence = stop_sequence
        self.stop_headsign = stop_headsign
        self.pickup_type = pickup_type
        self.drop_off_type = drop_off_type
        self.timepoint = timepoint


### Crear Tablas
engine = create_engine("postgresql://postgres:5314806Jair@localhost:5432/gtfs")
Base.metadata.create_all(bind=engine)

### Agregar Elemento a la BD
# Session = sessionmaker(bind=engine)
# session = Session()
# zip = Zip(5333,"Abner Ramon Astete","prueba","Sin Procesar","En Espera")
# session.add(zip)
# session.commit()


### Probar Query a la BD
# resultado = session.query(Zip).all()
# print(resultado)

