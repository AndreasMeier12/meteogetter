import datetime
import math
from functools import lru_cache
from typing import Dict, Tuple

import pandas
import plotnine
import sqlalchemy
from dateutil.rrule import rrule, MONTHLY
from pandas import Timestamp
from pytz import timezone
from skyfield import api
from sqlalchemy.orm import sessionmaker

import models
from main import get_db_uri
from mytypes import MonthResult

ALIGNMENT_CUTOFF = datetime.timedelta(minutes=15)
LATITUDE = 47.4
LONGITUDE = 8.05
TIMEZONE = timezone("Europe/Berlin")
COLNAMES = ["temperature", "humidity", "dew_point"]
TOWN = api.wgs84.latlon(LATITUDE, LONGITUDE)
EPH = api.load("de421.bsp")
TIMESCALE = api.load.timescale()
BASE_VARIABLES = {
    "temperature": "Temperature / °C",
    "humidity": "Humidity %",
    "dew_point": "Dew point / °C",
}
SUFFIX_METEO = "_meteo"
SUFFIX_BALCONY = "_balcony"
from skyfield import almanac


def is_night(row: pandas.Series) -> bool:
    timestamp = row["timestamp"]
    timestamp_localized = TIMEZONE.localize(timestamp)
    sunrise, sunset = (
        Timestamp(ts_input=x.utc_datetime()) for x in find_sunrise_sunset(timestamp)[0]
    )
    return timestamp_localized < sunrise or timestamp_localized > sunset


def is_morning(row: pandas.Series) -> bool:
    timestamp = row["timestamp"]
    timestamp_localized = TIMEZONE.localize(timestamp)
    sunrise, sunset = (
        Timestamp(ts_input=x.utc_datetime()) for x in find_sunrise_sunset(timestamp)[0]
    )
    noon = sunrise + ((sunset - sunrise) / 2)
    return sunrise < timestamp_localized < noon

    sunrise, sunset = (
        Timestamp(ts_input=x.utc_datetime()) for x in find_sunrise_sunset(timestamp)[0]
    )


def is_afternoon(row: pandas.Series) -> bool:
    timestamp = row["timestamp"]
    timestamp_localized = TIMEZONE.localize(timestamp)
    sunrise, sunset = (
        Timestamp(ts_input=x.utc_datetime()) for x in find_sunrise_sunset(timestamp)[0]
    )
    noon = sunrise + ((sunset - sunrise) / 2)
    return noon < timestamp_localized < sunset


@lru_cache()
def find_sunrise_sunset(timestamp: Timestamp):
    start = TIMESCALE.from_datetime(
        TIMEZONE.localize(
            Timestamp(year=timestamp.year, month=timestamp.month, day=timestamp.day)
        )
    )
    end = TIMESCALE.from_datetime((start + pandas.Timedelta(days=1)).utc_datetime())
    return find_sunrise_sunset_helper(start, end)


@lru_cache
def find_sunrise_sunset_helper(start: datetime.datetime, end: datetime.datetime):
    return almanac.find_discrete(start, end, almanac.sunrise_sunset(EPH, TOWN))


def calculate_crude_dew_point_for_row(row: pandas.Series):
    if math.isnan(row["temperature"]):
        return None
    if math.isnan(row["humidity"]):
        return None
    return row["temperature"] - ((100 - row["humidity"]) / 5)


def calculate_crude_dew_point(a: pandas.DataFrame):
    a["dew_point"] = a.apply(calculate_crude_dew_point_for_row, axis=1)


def pair_closest_datapoint_in_time(
    a: pandas.Series, a_name: str, b: pandas.DataFrame, colnames: [str]
) -> pandas.Series:
    timestamp = a["timestamp"]
    start = timestamp - ALIGNMENT_CUTOFF
    end = timestamp + ALIGNMENT_CUTOFF
    candidates = b.query("timestamp > @start and timestamp < @end")
    if len(candidates) == 0:
        return None
    else:
        asdf = candidates.loc[[b["timestamp"].sub(timestamp).abs().idxmin()]]
        res = {}
        for colname in colnames:
            res[f"{colname}_{a_name}"] = a[colname]
            res[f"{colname}_{b.name}"] = (asdf[colname]).values[0]
            res[f"delta_{colname}"] = (asdf[colname] - a[colname]).values[0]
        res["timestamp"] = timestamp
        return pandas.Series(res)


def match_values(
    a: pandas.DataFrame, b: pandas.DataFrame, colnames: [str]
) -> pandas.DataFrame:
    temp = [
        pair_closest_datapoint_in_time(x[1], a.name, b, colnames) for x in a.iterrows()
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


def filter_df_by_month(df: pandas.DataFrame, m: int, y: int):
    df_a = df[df["timestamp"].dt.month == m]
    df_f = df_a[df_a["timestamp"].dt.year == y]
    df_f.name = meteo.name
    return df_f


def split_by_month_and_year(df: pandas.DataFrame) -> Dict[Tuple[int, int], MonthResult]:
    start, end = min(df["timestamp"]), max(df["timestamp"])
    diff_month = lambda e, s: (e.year - s.year) * 12 + e.month - s.month

    months = [
        (d.month, d.year)
        for d in rrule(MONTHLY, count=diff_month(end, start) + 1, dtstart=start)
    ]
    return {(m, y): MonthResult(filter_df_by_month(df, m, y), m, y) for m, y in months}


def plot_by_month(balcony: pandas.DataFrame, meteo: pandas.DataFrame):
    matched_vals = match_values(
        meteo, balcony, ["temperature", "humidity", "dew_point"]
    )
    meteo_by_months = split_by_month_and_year(meteo)
    balcony_by_month = split_by_month_and_year(balcony)
    all_months = sorted(list(set(meteo_by_months).union(set(balcony_by_month))))
    plot_matched(matched_vals)

    plot_histogram_by_daytime(matched_vals, "delta_temperature", "Δ Temperature /°C")
    plot_histogram_by_daytime(matched_vals, "delta_humidity", "Δ Humidity %")
    plot_histogram_by_daytime(matched_vals, "delta_dew_point", "Δ Dew Point /°C")


def melt_concat(
    meteo: pandas.DataFrame, balcony: pandas.DataFrame, colname: str
) -> pandas.DataFrame:
    m = meteo.filter(items=["timestamp", colname])
    m["type"] = "meteo"
    b = balcony.filter(items=["timestamp", colname])
    b["type"] = "balcony"
    c = pandas.concat([m, b])
    return c.melt(id_vars=["timestamp", "type"])


def time_melt(a: pandas.DataFrame, type) -> pandas.DataFrame:
    b = a.copy()
    b["type"] = type
    return b.melt(id_vars=["timestamp", "type"])


def plot_line(melted: pandas.DataFrame, colname):
    asdf = (
        plotnine.ggplot(
            melted, plotnine.aes(x="timestamp", y="value", color="type", group="type")
        )
        + plotnine.geom_line()
        + plotnine.theme(
            axis_text_x=plotnine.element_text(angle=90), figure_size=(16, 8)
        )
        + plotnine.ylab(BASE_VARIABLES[colname])
        + plotnine.ggtitle(BASE_VARIABLES[colname])
        + plotnine.facet_wrap("~ year + month", scales="free_x")
    )
    asdf.draw()
    return asdf


def plot_line_nonmelted(a: pandas.DataFrame, colname: str, label: str, m: int, y: int):
    asdf = (
        plotnine.ggplot(a, plotnine.aes(x="timestamp", y=colname))
        + plotnine.geom_line()
        + plotnine.theme(axis_text_x=plotnine.element_text(angle=90))
        + plotnine.ggtitle(f"{label} {y}-{m}")
    )
    asdf.draw()


def plot_histogram_by_daytime(
    a: pandas.DataFrame,
    colname: str,
    label_name: str,
) -> None:
    if a.empty:
        return
    b = a.copy()
    b["year"] = b.timestamp.dt.year
    b["month"] = b.timestamp.dt.month
    plot_hists(
        b[b.apply(lambda x: is_morning(x), axis=1)], colname, label_name, "morning"
    )
    plot_hists(
        b[b.apply(lambda x: is_afternoon(x), axis=1)], colname, label_name, "afternoon"
    )
    plot_hists(b[b.apply(lambda x: is_night(x), axis=1)], colname, label_name, "night")


def tidy_matched(a: pandas.DataFrame, colname: str) -> pandas.DataFrame:
    b = a.melt(id_vars=["timestamp"])
    meteo = b.query("variable==" + "'" + colname + SUFFIX_METEO + "'")
    balcony = b.query("variable==" + "'" + colname + SUFFIX_BALCONY + "'")
    meteo["type"] = "meteo"
    balcony["variable"] = colname
    balcony["type"] = "balcony"
    meteo["variable"] = colname
    c = pandas.concat([meteo, balcony])
    c["year"] = c.timestamp.dt.year
    c["month"] = c.timestamp.dt.month
    return c


def plot_matched(
    a: pandas.DataFrame,
) -> None:
    if a.empty:
        return
    b = a.copy()
    b["year"] = b.timestamp.dt.year
    b["month"] = b.timestamp.dt.month
    c = [(tidy_matched(b, k), k) for k, v in BASE_VARIABLES.items()]
    plots = [plot_line(x, y) for x, y in c]

    return plots


def plot_hists(c: pandas.DataFrame, colname: str, label_name: str, daytime: str):
    plot = (
        plotnine.ggplot(data=c)
        + plotnine.aes(x=colname)
        + plotnine.geom_histogram()
        + plotnine.ggtitle(f"morning {colname}")
        + plotnine.xlab(label_name)
        + plotnine.facet_wrap("~ year + month")
        + plotnine.themes.theme_dark()
    )
    plot.draw()


if __name__ == "__main__":
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
