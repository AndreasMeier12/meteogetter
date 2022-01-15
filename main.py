import os
import urllib
import urllib.error
import urllib.request
from sqlite3 import Cursor

import aiohttp
import asyncio
import chardet
import csv
import io
import sqlite3
import sqlalchemy
import pandas

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


def main(name):
    res = asyncio.run(run([url_temp, url_humidity]))
    engine = sqlalchemy.create_engine(get_db_uri())

    # Use a breakpoint in the code line below to debug your script.
    dataframes = read_respone(res)

    print(f"Hi, {name}")  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    main("PyCharm")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
