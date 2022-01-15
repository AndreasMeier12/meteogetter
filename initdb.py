from sqlalchemy import engine, create_engine


def __main__():
    from models import Base, Station, TemperatureMeasurement, HumidityMeasurement
    from main import get_db_uri

    uri = get_db_uri()

    engine = create_engine(uri)
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    __main__()
