import urllib
import urllib.error
import urllib.request
import aiohttp
import asyncio
import chardet
import csv
import io

import pandas

NAPTIME = 60 * 60

url_temp = 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-lufttemperatur-10min/ch.meteoschweiz.messwerte-lufttemperatur-10min_en.csv'
url_humidity = 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-luftfeuchtigkeit-10min/ch.meteoschweiz.messwerte-luftfeuchtigkeit-10min_en.csv '

def parse_to_dataframe(b):

        c = b[2]
        d = c.decode(encoding, encoding='latin-1')
        e = '\n'.join(d[:-5])
        f = pandas.read_csv(io.StringIO(e), skiprows=0, sep=';')
        reader = csv.reader(d)

def read_respone(x: asyncio.Task):
    a = x.result()
    res = [parse_to_dataframe(x) for x in a]
    return res





async def fetch(url, session):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0'}
    try:
        async with session.get(
            url, headers=headers,
            ssl = False,
            timeout = aiohttp.ClientTimeout(
                total=None,
                sock_connect = 10,
                sock_read = 10
            )
        ) as response:
            content = await response.read()
            return (url, 'OK', content)
    except Exception as e:
        print(e)
        return (url, 'ERROR', str(e))

async def run(url_list):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in url_list:
            task = asyncio.ensure_future(fetch(url, session))
            tasks.append(task)
        responses = asyncio.gather(*tasks)
        await responses
    return responses

def main(name):
    res = asyncio.run(run([url_temp, url_humidity]))
    # Use a breakpoint in the code line below to debug your script.
    read_respone(res)





    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
