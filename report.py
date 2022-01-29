import datetime

import matplotlib.pyplot as plt
import pandas
import sqlalchemy
from sqlalchemy.orm import sessionmaker

import models
from main import get_db_uri

ALIGNMENT_CUTOFF = datetime.timedelta(minutes=15)


def get_min_by_month():
    pass


def get_max_by_month():
    pass


def pair_closest_datapoint_in_time(
    a: pandas.Series, a_name: str, b: pandas.DataFrame, colname: str
) -> pandas.Series:
    timestamp = a["timestamp"]
    start = timestamp - ALIGNMENT_CUTOFF
    end = timestamp + ALIGNMENT_CUTOFF
    candidates = b.query("timestamp > @start and timestamp < @end")
    if len(candidates) == 0:
        return None
    else:
        asdf = candidates.loc[[b["timestamp"].sub(timestamp).abs().idxmin()]]
        return pandas.Series(
            {
                "timestamp": timestamp,
                f"{colname}_{a_name}": a[colname],
                f"{colname}_{b.name}": asdf[colname],
                f"delta_{colname}": asdf[colname].values[0] - a[colname],
            }
        )


def match_values(
    a: pandas.DataFrame, b: pandas.DataFrame, colname: str
) -> pandas.DataFrame:
    temp = [
        pair_closest_datapoint_in_time(x[1], a.name, b, colname) for x in a.iterrows()
    ]
    return pandas.DataFrame.from_records([x for x in temp if x is not None])


def query_db_to_dataframe(session, cls_to_query) -> pandas.DataFrame:
    vals = session.query(cls_to_query).filter(cls_to_query.station_id == 24).all()
    datty_frame: pandas.DataFrame = pandas.DataFrame.from_records(
        [x.to_dict() for x in vals]
    )
    return datty_frame


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
        meteo_temps: pandas.DataFrame = query_db_to_dataframe(
            session, models.TemperatureMeasurement
        )
        meteo_humidities: pandas.DataFrame = query_db_to_dataframe(
            session, models.HumidityMeasurement
        )
        meteo_humidities.name = "meteo"

        balcony = pandas.read_csv("BALCONY.CSV")
        balcony["timestamp"] = pandas.to_datetime(balcony["timestamp"])
        balcony.name = "balcony"
        meteo_temps["timestamp"] = pandas.to_datetime(meteo_temps["timestamp"])
        meteo_temps.name = "meteo"
        ax = meteo_temps.plot(x="timestamp", y="temperature")
        balcony.plot(x="timestamp", y="temperature", ax=ax)

        # balcony.Timestamp.groupby(pandas.Grouper(freq='M'))
        plt.show()

        matched_temps = match_values(meteo_temps, balcony, "temperature")
        matched_temps.plot(x="timestamp", y="delta_temperature")
        plt.show()

        matched_humidities = match_values(meteo_humidities, balcony, "humidity")
        matched_humidities.plot(x="timestamp", y="delta_humidity")
        plt.show()
        print("asdf")
