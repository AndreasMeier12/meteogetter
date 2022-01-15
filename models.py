from sqlalchemy.orm import declarative_base

Base = declarative_base()
from sqlalchemy import Column, Integer, String, Float, ForeignKey


class Station(Base):
    __tablename__ = "station"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    abbr = Column(String, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)
    altitude = Column(Float)

    def __repr__(self):
        return f"Station {self.id} {self.name} {self.abbr}"


class TemperatureMeasurement(Base):
    __tablename__ = "temperature_measurement"

    station_id = Column(Integer, ForeignKey("station.id"), primary_key=True)
    timestamp = Column(Integer, primary_key=True)
    value = Column(Float)

    def __repr__(self):
        return f"Station {self.station_id} temp at {self.timestamp}: {self.value}"


class HumidityMeasurement(Base):
    __tablename__ = "humidity_measurement"

    station_id = Column(Integer, ForeignKey("station.id"), primary_key=True)
    timestamp = Column(Integer, primary_key=True)
    value = Column(Float)

    def __repr__(self):
        return f"Station {self.station_id} humidity at {self.timestamp}: {self.value}"
