import asyncio
import io
import logging
import os
from sqlite3 import Cursor
from time import sleep

import aiohttp
import dateutil
import pandas
import sqlalchemy
from sqlalchemy.orm import sessionmaker

import models

NAPTIME = 60 * 60

url_temp = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-lufttemperatur-10min/ch.meteoschweiz.messwerte-lufttemperatur-10min_en.csv"
url_humidity = "https://data.geo.admin.ch/ch.meteoschweiz.messwerte-luftfeuchtigkeit-10min/ch.meteoschweiz.messwerte-luftfeuchtigkeit-10min_en.csv"


def parse_to_dataframe(b):

    c = b[2]
    d = c.decode("latin-1")
    e = d.split("\n")
    f = "\n".join(e[:-5])
    g = pandas.read_csv(io.StringIO(f), skiprows=0, sep=";")
    return g


def read_respone(x: asyncio.Task):
    a = x.result()
    res = [parse_to_dataframe(x) for x in a]
    return res


async def fetch(url, session):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0"
    }
    try:
        async with session.get(
            url,
            headers=headers,
            ssl=False,
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=10),
        ) as response:
            content = await response.read()
            return (url, "OK", content)
    except Exception as e:
        print(e)
        return (url, "ERROR", str(e))


async def run(url_list):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in url_list:
            task = asyncio.ensure_future(fetch(url, session))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses
    return responses


def write_stations(a: [pandas.DataFrame], cur: Cursor) -> dict:
    asdf = cur.execute("SELECT * FROM station").fetchall()
    dict = {}
    max_id = max([x[0] for x in asdf])


def get_db_uri():
    basedir = os.path.abspath(os.path.dirname(__file__))
    return os.environ.get("DATABASE_URL") or "sqlite:///" + os.path.join(
        basedir, "meteo.db"
    )


def get_all_stations(session):
    session.query(models.Station)
    return []


def handle_res():

    return None


def test_read():
    with open("22015_0800_temp.csv", "r") as temp, open(
        "22015_0800_humid.csv"
    ) as humid:
        a = pandas.read_csv(temp)
        b = pandas.read_csv(humid)
        return [a, b]


def data_row_to_station(a):
    x = None
    try:
        b = a[1]
        x = models.Station(
            name=b["Station"],
            abbr=b["Abbr."],
            latitude=b["Latitude"],
            longitude=b["Longitude"],
            altitude=b["Measurement height m. a. sea level"],
            height=b["Measurement height m. a. sea level"],
        )
        return x
    except Exception as e:
        return None


def handle_stations(a, session) -> dict:
    known_stations = session.query(models.Station).all()
    stations = [data_row_to_station(x) for x in a[0].iterrows()]
    known_abbrs = [x.abbr for x in known_stations]
    new_stations = [x for x in stations if x.abbr not in known_abbrs]
    session.add_all(new_stations)
    session.commit()
    stations: [models.Station] = session.query(models.Station).all()

    return {x.abbr: x.id for x in stations}


def handle_humidity_row(a, station_ids: dict):
    b = a[1]
    id = station_ids[b["Abbr."]]
    timestamp = dateutil.parser.parse(b["Measurement date"])
    val = b["Humidity %"]

    return models.HumidityMeasurement(station_id=id, timestamp=timestamp, value=val)


def handle_temp_row(a, station_ids: dict):
    b = a[1]
    id = station_ids[b["Abbr."]]
    timestamp = dateutil.parser.parse(b["Measurement date"])
    val = b["Temperature °C"]
    return models.TemperatureMeasurement(station_id=id, timestamp=timestamp, value=val)


def handle_humidity(a, station_ids: dict):
    return [handle_humidity_row(x, station_ids) for x in a.iterrows()]


def handle_temp(a, station_ids: dict):
    return [handle_temp_row(x, station_ids) for x in a.iterrows()]


def get_handler(a: pandas.DataFrame):
    if any("Temperature" in x for x in a.columns):
        return handle_temp
    if any("Humidity" in x for x in a.columns):
        return handle_humidity


def handle_measurements(a: [pandas.DataFrame], station_ids: dict, session):
    vals = []
    for asdf in a:
        handler = get_handler(asdf)
        vals = vals + handler(asdf, station_ids)
    session.add_all(vals)
    session.commit()


def main(name):
    engine = sqlalchemy.create_engine(get_db_uri())
    Session = sessionmaker(bind=engine)
    session = Session()
    logging.basicConfig(
        filename="log.txt",
        format="%(asctime)s %(levelname)-2s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    try:
        # Use a breakpoint in the code line below to debug your script.
        res = asyncio.run(run([url_temp, url_humidity]))
        dataframes = read_respone(res)
        station_ids = handle_stations(dataframes, session)
        handle_measurements(dataframes, station_ids, session)
        logging.info("Successfuly get")
    except Exception as e:
        logging.error(f"Error in getting {e}")
    logging.info(f"Sleeping for {NAPTIME}")


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    main("PyCharm")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
