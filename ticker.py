#import csv
import asyncio
import requests
from time import sleep
from datetime import datetime
from os import getcwd


async def poloniex(current_time):
    print(str(datetime.now()) + 'poloniex START')
    with requests.get('https://poloniex.com/public?command=returnTicker') as response:
        text = response.text
        with open(getcwd() + '/ticker/poloniex.csv', 'a+') as f:
            f.write(current_time+','+text+'\n')
    print(str(datetime.now()) + 'poloniex FINISH')


async def binance(current_time):
    print(str(datetime.now()) + 'binance START')
    with requests.get('https://api.binance.com/api/v3/ticker/bookTicker') as response:
        text = response.text
        with open(getcwd() + '/ticker/binance.csv', 'a+') as f:
            f.write(current_time + ',' + text + '\n')
    sleep(1)
    print(str(datetime.now()) + 'binance FINISH')


async def bitfinex(current_time):
    print(str(datetime.now()) + 'bitfinex START')
    with requests.get('https://api-pub.bitfinex.com/v2/tickers?symbols=ALL') as response:
        text = response.text
        with open(getcwd() + '/ticker/bitfinex.csv', 'a+') as f:
            f.write(current_time + ',' + text + '\n')
    print(str(datetime.now()) + 'bitfinex FINISH')

def main():

    counter = 0
    while counter < 10:
        ct = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        loop = asyncio.get_event_loop()
        loop.set_exception_handler(lambda x, y: None)  # suppress exceptions because of bug in Python 3.7.3 + aiohttp + asyncio
        loop.run_until_complete(asyncio.ensure_future(asyncio.gather(poloniex(ct), binance(ct), bitfinex(ct))))

        sleep(1)
        counter += 1
        if counter % 1 == 0:
            print(str(counter))


if __name__ == '__main__':
    main()