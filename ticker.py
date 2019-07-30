#import csv
import asyncio
import requests
from time import sleep
from datetime import datetime
from os import getcwd

CURRENT_TIME = ""


async def poloniex():

    with requests.get('https://poloniex.com/public?command=returnTicker') as response:
        text = await response.text
        with open(getcwd() + '/ticker/poloniex.csv', 'a+') as f:
            f.write(CURRENT_TIME+','+text+'\n')
        print(CURRENT_TIME + 'Poloniex')


async def binance():

    with requests.get('https://api.binance.com/api/v3/ticker/bookTicker') as response:
        text = await response.text
        with open(getcwd() + '/ticker/bitfinex.csv', 'a+') as f:
            f.write(CURRENT_TIME + ',' + text + '\n')
        print(CURRENT_TIME + 'binance')


async def bitfinex():
    with requests.get('https://api-pub.bitfinex.com/v2/tickers?symbols=ALL') as response:
        text = await response.text
        with open(getcwd() + '/ticker/bitfinex.csv', 'a+') as f:
            f.write(CURRENT_TIME + ',' + text + '\n')
        print(CURRENT_TIME + 'binance')


def main():

    counter = 0
    while counter < 10:
        CURRENT_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        loop = asyncio.get_event_loop()
        loop.set_exception_handler(lambda x, y: None)  # suppress exceptions because of bug in Python 3.7.3 + aiohttp + asyncio
        loop.run_until_complete(asyncio.ensure_future(asyncio.gather(poloniex(), binance(), bitfinex())))

        sleep(1)
        counter += 1
        if counter % 1 == 0:
            print(str(counter))


if __name__ == '__main__':
    main()