#import csv
import asyncio
import requests
from time import sleep
from datetime import datetime
from os import getcwd


def gather_ticker():

    #async def poloniex():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async with requests.get('https://poloniex.com/public?command=returnTicker') as response:
        text = await response.text
        with open(getcwd() + '/ticker/poloniex.csv', 'a+') as f:
            f.write(current_time+','+text+'\n')
        print(current_time + 'Poloniex')

    #async def binance():
    async with requests.get('https://api.binance.com/api/v3/ticker/bookTicker') as response:
        text = await response.text
        with open(getcwd() + '/ticker/bitfinex.csv', 'a+') as f:
            f.write(current_time + ',' + text + '\n')
        print(current_time + 'binance')

    #async def bitfinex():
    async with requests.get('https://api-pub.bitfinex.com/v2/tickers?symbols=ALL') as response:
        text = await response.text
        with open(getcwd() + '/ticker/bitfinex.csv', 'a+') as f:
            f.write(current_time + ',' + text + '\n')
        print(current_time + 'binance')


def main():

    counter = 0
    while counter < 10:
        gather_ticker()
        sleep(1)
        counter += 1
        if counter % 10 == 0:
            print(str(counter))

if __name__ == '__main__':
    main()