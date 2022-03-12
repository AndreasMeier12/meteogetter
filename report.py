import datetime

import matplotlib.pyplot as plt
import pandas
import sqlalchemy
from dateutil.rrule import rrule, MONTHLY
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
    ).drop(columns=["station_id"])
    meteo_humidities: pandas.DataFrame = query_db_to_dataframe(
        session, models.HumidityMeasurement
    ).drop(columns=["station_id"])
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


def statsify(a: pandas.DataFrame):
    b = a.resample("M", on="timestamp").mean()
    c = a.resample("D", on="timestamp").mean().resample("M").min()
    d = a.resample("D", on="timestamp").mean().resample("M").max()
    return b.join(c, rsuffix="_low").join(d, rsuffix="_high")


def print_stats(a: pandas.DataFrame, b: pandas.DataFrame) -> None:
    a_new = statsify(a)
    b_new = statsify(b)
    deltas = a_new.sub(b_new)
    deltas.name = "deltas"
    a_new.name = a.name
    b_new.name = b.name

    print(a_new)
    print(b_new)
    print(deltas)


def plot_by_month(balcony: pandas.DataFrame, meteo: pandas.DataFrame):
    start, end = min(meteo["timestamp"]), max(meteo["timestamp"])
    diff_month = lambda e, s: (e.year - s.year) * 12 + e.month - s.month

    months = [
        (d.month, d.year)
        for d in rrule(MONTHLY, count=diff_month(end, start) + 1, dtstart=start)
    ]
    for m, y in months:
        meteo_a = meteo[meteo["timestamp"].dt.month == m]
        meteo_f = meteo_a[meteo_a["timestamp"].dt.year == y]
        meteo_f.name = meteo.name

        balcony_a = balcony[balcony["timestamp"].dt.month == m]
        balcony_f = balcony_a[balcony_a["timestamp"].dt.month == m]
        balcony_f.name = balcony.name
        ax = meteo_f.plot(x="timestamp", y="temperature")
        balcony_f.plot(x="timestamp", y="temperature", ax=ax)
        ax.set_ylabel("Temperature /°C")
        ax.set_xlabel("Date")
        plt.title = f"{y} - {m}"
        plt.show()

        matched_temps = match_values(meteo_f, balcony_f, "temperature")
        plot_matched(matched_temps, "delta_temperature", "Δ Temperature /°C", m=m, y=y)

        matched_humidities = match_values(meteo_f, balcony_f, "humidity")
        plot_matched(
            matched_humidities, "delta_humidity", "Δ Realitive humidity /%", m=m, y=y
        )

        matched_humidities = match_values(meteo_f, balcony_f, "humidity")
        plot_matched(
            matched_humidities, "delta_humidity", "Δ Realitive humidity /%", m=m, y=y
        )

        matched_dew_points = match_values(meteo_f, balcony_f, "dew_point")
        plot_matched(matched_dew_points, "delta_dew_point", "Δ dew_point /°C", m=m, y=y)


def plot_matched(
    a: pandas.DataFrame, colname: str, label_name: str, y: int = None, m: int = None
) -> None:
    if a.empty:
        return
    ax = a.plot(x="timestamp", y=colname)
    ax.set_ylabel(label_name)
    ax.set_xlabel("Date")
    if y and m:
        plt.title = f"{y} - {m}"
    plt.show()


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
        plot_by_month(balcony, meteo)

        print_stats(balcony, meteo)
