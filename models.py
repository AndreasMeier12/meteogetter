import dateutil
from sqlalchemy.orm import declarative_base

Base = declarative_base()
from sqlalchemy import Column, Integer, String, Float, ForeignKey, orm


class Station(Base):
    __tablename__ = "station"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    abbr = Column(String, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)
    altitude = Column(Float)
    height = Column(String)

    def __repr__(self):
        return f"Station {self.id} {self.name} {self.abbr}"


class TemperatureMeasurement(Base):
    __tablename__ = "temperature_measurement"

    station_id = Column(Integer, ForeignKey("station.id"), primary_key=True)
    timestamp = Column(Integer, primary_key=True)
    value = Column(Float)

    def __repr__(self):
        return f"Station {self.station_id} temp at {self.timestamp}: {self.value}"

    @orm.reconstructor
    def init_on_load(self):
        self.timestamp = dateutil.parser.parse(self.timestamp)

    def to_dict(self):
        return {
            "temperature": self.value,
            "timestamp": self.timestamp,
            "station_id": self.station_id,
        }


class HumidityMeasurement(Base):
    __tablename__ = "humidity_measurement"

    station_id = Column(Integer, ForeignKey("station.id"), primary_key=True)
    timestamp = Column(Integer, primary_key=True)
    value = Column(Float)

    @orm.reconstructor
    def init_on_load(self):
        self.timestamp = dateutil.parser.parse(self.timestamp)

    def __repr__(self):
        return f"Station {self.station_id} humidity at {self.timestamp}: {self.value}"

    def to_dict(self):
        return {
            "humidity": self.value,
            "timestamp": self.timestamp,
            "station_id": self.station_id,
        }


class PrecipitationMeasurement(Base):
    __tablename__ = "precipitation_measurement"

    station_id = Column(Integer, ForeignKey("station.id"), primary_key=True)
    timestamp = Column(Integer, primary_key=True)
    value = Column(Float)

    @orm.reconstructor
    def init_on_load(self):
        self.timestamp = dateutil.parser.parse(self.timestamp)

    def __repr__(self):
        return f"Station {self.station_id} preiciptation at {self.timestamp}: {self.value} mm"

    def to_dict(self):
        return {
            "humidity": self.value,
            "timestamp": self.timestamp,
            "station_id": self.station_id,
        }


class WindMeasurement(Base):
    __tablename__ = "wind_measurement"

    station_id = Column(Integer, ForeignKey("station.id"), primary_key=True)
    timestamp = Column(Integer, primary_key=True)
    value = Column(Float)
    direction = Column(Float)

    @orm.reconstructor
    def init_on_load(self):
        self.timestamp = dateutil.parser.parse(self.timestamp)

    def __repr__(self):
        return f"Station {self.station_id} windspeed, direction at {self.timestamp}: {self.value}, {self.direction}"

    def to_dict(self):
        return {
            "windspeed": self.value,
            "winddirection": self.direction,
            "timestamp": self.timestamp,
            "station_id": self.station_id,
        }
