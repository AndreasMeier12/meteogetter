import datetime

import matplotlib.pyplot as plt
import pandas
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import math

import models
from main import get_db_uri

ALIGNMENT_CUTOFF = datetime.timedelta(minutes=15)


def get_min_by_month():
    pass


def get_max_by_month():
    pass


def calculate_crude_dew_point_for_row(row: pandas.Series):
    if math.isnan(row["temperature"]):
        return None
    if math.isnan(row["humidity"]):
        return None
    return row["temperature"] - ((100 - row["humidity"]) / 5)


def calculate_crude_dew_point(a: pandas.DataFrame):
    a["dew_point"] = a.apply(calculate_crude_dew_point_for_row, axis=1)


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
    datty_frame["timestamp"] = pandas.to_datetime(datty_frame["timestamp"])
    return datty_frame


def fetch_meteo_dataframe(session) -> pandas.DataFrame:
    meteo_temps: pandas.DataFrame = query_db_to_dataframe(
        session, models.TemperatureMeasurement
    )
    meteo_humidities: pandas.DataFrame = query_db_to_dataframe(
        session, models.HumidityMeasurement
    )
    res = meteo_humidities.merge(meteo_temps, on="timestamp", how="left")
    res.name = "meteo"
    calculate_crude_dew_point(res)
    return res


def read_balcony_data():
    balcony = pandas.read_csv("BALCONY.CSV")
    balcony["timestamp"] = pandas.to_datetime(balcony["timestamp"])
    balcony.name = "balcony"
    calculate_crude_dew_point(balcony)
    return balcony


def print_stats(a: pandas.DataFrame) -> None:
    print(a.resample("M", on="timestamp").mean())
    print(a.resample("D", on="timestamp").mean().resample("M").min())
    print(a.resample("D", on="timestamp").mean().resample("M").max())


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
        meteo = fetch_meteo_dataframe(session)

        balcony = read_balcony_data()
        ax = meteo.plot(x="timestamp", y="temperature")
        balcony.plot(x="timestamp", y="temperature", ax=ax)
        # balcony.Timestamp.groupby(pandas.Grouper(freq='M'))
        ax.set_ylabel("Temperature /°C")
        ax.set_xlabel("Date")
        plt.show()

        matched_temps = match_values(meteo, balcony, "temperature")
        ax = matched_temps.plot(x="timestamp", y="delta_temperature")
        ax.set_ylabel("Δ Temperature /°C")
        ax.set_xlabel("Date")
        plt.show()

        matched_humidities = match_values(meteo, balcony, "humidity")
        ax = matched_humidities.plot(x="timestamp", y="delta_humidity")
        ax.set_ylabel("Realitive humidity /%")
        ax.set_xlabel("Date")
        plt.show()

        matched_dewpoints = match_values(meteo, balcony, "dew_point")
        ax = matched_dewpoints.plot(x="timestamp", y="delta_dew_point")
        ax.set_ylabel("Δ Temperature /°C")
        ax.set_xlabel("Date")
        plt.show()

        print_stats(balcony)
