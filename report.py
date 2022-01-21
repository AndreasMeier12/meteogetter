import sqlalchemy
from sqlalchemy.orm import sessionmaker
import models
import pandas
import matplotlib.pyplot as plt

from main import get_db_uri


if __name__ == "__main__":
    with plt.style.context("dark_background"):
        engine = sqlalchemy.create_engine(get_db_uri())
        Session = sessionmaker(bind=engine)
        session = Session()
        temps = (
            session.query(models.TemperatureMeasurement)
            .filter(models.TemperatureMeasurement.station_id == 24)
            .all()
        )
        datty_frame: pandas.DataFrame = pandas.DataFrame.from_records(
            [x.to_dict() for x in temps]
        )
        datty_frame.plot(x="timestamp", y="value")
        plt.show()
        print("asdf")
