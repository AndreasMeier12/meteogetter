import datetime

import sqlalchemy
from sqlalchemy.orm import sessionmaker
import models
import pandas
import matplotlib.pyplot as plt

from main import get_db_uri

ALIGNMENT_CUTOFF = datetime.timedelta(minutes=15)


def get_min_by_month():
    pass


def get_max_by_month():
    pass


def find_closest_datapoint_in_time(
    a: pandas.Series, b: pandas.DataFrame
) -> pandas.Series:
    timestamp = a["timestamp"]
    start = timestamp - ALIGNMENT_CUTOFF
    end = timestamp + ALIGNMENT_CUTOFF
    candidates = b.query("timestamp > @start and timestamp < @end")
    if len(candidates) == 0:
        return None
    else:
        asdf = candidates.loc[[b["timestamp"].sub(timestamp).abs().idxmin()]]
        return


def match_values(
    a: pandas.DataFrame, b: pandas.DataFrame, colname: str
) -> pandas.DataFrame:
    return [find_closest_datapoint_in_time(x[1], b) for x in a.iterrows()]


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
        balcony = pandas.read_csv("BALCONY.CSV")
        balcony["timestamp"] = pandas.to_datetime(balcony["timestamp"])
        datty_frame["timestamp"] = pandas.to_datetime(datty_frame["timestamp"])
        ax = datty_frame.plot(x="timestamp", y="temperature")
        balcony.plot(x="timestamp", y="temperature", ax=ax)
        # balcony.Timestamp.groupby(pandas.Grouper(freq='M'))
        plt.show()
        match_values(datty_frame, balcony, "temperature")
        print("asdf")
