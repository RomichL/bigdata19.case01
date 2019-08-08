import aiofiles
from aiohttp import ClientSession
import asyncio
from collections import defaultdict
from json import dumps, loads
import pyarrow as pa
import pyarrow.parquet as pq
from time import time, sleep, strftime, gmtime
from os import getcwd, listdir

TICKERS_DIR = getcwd()+'/ticker/'


tickers_symbols = ('USDBTC', 'USDETH', 'USDXRP', 'USDLTC', 'ETHBTC', 'LTCBTC', 'XRPBTC')

poloniex_ticker_map = {'USDBTC': 'USDT_BTC', 'USDETH': 'USDT_ETH', 'USDXRP': 'USDT_XRP', 'USDLTC': 'USDT_LTC',
                       'ETHBTC': 'BTC_ETH', 'LTCBTC': 'BTC_LTC', 'XRPBTC': 'BTC_XRP'}

binance_ticker_map = {'USDBTC': 'TUSDBTC', 'USDETH': 'TUSDETH', 'USDXRP': 'XRPUSDT', 'USDLTC': 'LTCUSDT',
                      'ETHBTC': 'ETHBTC', 'LTCBTC': 'LTCBTC', 'XRPBTC': 'XRPBTC'}

bitfinex_ticker_map = {'USDBTC': 'tBTCUSD', 'USDETH': 'tETHUSD', 'USDXRP': 'tXRPUSD', 'USDLTC': 'tLTCUSD',
                       'ETHBTC': 'tETHBTC', 'LTCBTC': 'tLTCBTC', 'XRPBTC': 'tXRPBTC'}


def array_of_dicts_to_str_rows(array):
    output = ''
    counter = 0
    for row in array:
        counter += 1
        output += dumps(row)
        if counter < len(array):
            output += '\n'
    return output


def find_rate_in_ticker(json_object, field_name, search_name, rate_name):
    for obj in json_object:
        if obj[field_name] == search_name:
            return obj[rate_name]


def data_from_ticker(exchange_name, ticker, current_time):
    data = []
    for symbol in tickers_symbols:
        if exchange_name == 'poloniex':
            rate = ticker[poloniex_ticker_map[symbol]]['last']
        elif exchange_name == 'binance':
            rate = find_rate_in_ticker(ticker, 'symbol', binance_ticker_map[symbol], 'price')
        elif exchange_name == 'bitfinex':
            rate = find_rate_in_ticker(ticker, 0, bitfinex_ticker_map[symbol], 1)
        else:
            continue

        data.append({'timestamp': current_time, 'exchange_name': exchange_name, 'pair': symbol, 'rate': rate})
    return data


async def get_ticker(exchange_name, ticker_url, current_time):
    #print(strftime('%Y-%m-%d %H:%M:%S', gmtime(current_time)) + f'  - {exchange_name} - START')
    async with ClientSession() as session:
        async with session.get(ticker_url) as response:
            response_json = await response.json()
            data_array = array_of_dicts_to_str_rows(data_from_ticker(exchange_name, response_json, current_time))
            #data_dict = data_from_ticker(exchange_name, response_json, current_time)
            date_string = strftime('%Y%m%d_%H%M', gmtime(current_time))
            async with aiofiles.open(TICKERS_DIR+f'ticker_{date_string}_{exchange_name}.txt', 'a+') as f:
                await f.write(data_array + '\n')
                #await f.write(dumps(data_dict))
    #print(strftime('%Y-%m-%d %H:%M:%S', gmtime(current_time)) + f'  - {exchange_name} -FINISH')


def from_ticker_to_parquet():

    names = ('timestamp', 'exchange_name', 'pair', 'rate')
    files = listdir(TICKERS_DIR)
    files.sort(reverse=True)
    count = 0

    for file_name in files:
        if file_name[:7] != 'ticker_':
            continue
        count += 1
        if count <= 3:
            continue #pass last 3 files (still can be open for writing)

        with open(TICKERS_DIR+'/'+file_name, 'r') as f:
            batch = defaultdict(list)
            data_string = f.read().replace('\n', ',')
            tickers = loads('['+data_string[:len(data_string)-1]+']')
            for t in tickers:
                for n in names:
                    if n == 'rate':
                        value = float(t[n])
                    else:
                        value = t[n]
                    batch[n].append(value)

            tables = pa.Table.from_arrays([pa.array(batch[n]) for n in names], names)
            with pq.ParquetWriter(TICKERS_DIR + '/ticker.parquet', tables.schema, use_dictionary=False, flavor={'spark'}) as writer:
                writer.write_table(tables)

def read_parquet():

    pf = pq.ParquetFile(TICKERS_DIR + '/ticker.parquet')

    for i in range(pf.metadata.num_row_groups):
        table = pf.read_row_group(i)
        columns = table.to_pydict()
        print(columns)

def main():

    counter = 0
    interval = 15
    duration = 5*60
    while counter <= duration/interval:
        ct = int(time())
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(lambda x, y: None)  # suppress exceptions because of bug in Python 3.7.3 + aiohttp + asyncio
        loop.run_until_complete(asyncio.ensure_future(asyncio.gather(
            get_ticker('poloniex', 'https://poloniex.com/public?command=returnTicker', ct),
            get_ticker('binance', 'https://api.binance.com/api/v3/ticker/price', ct),
            get_ticker('bitfinex', 'https://api-pub.bitfinex.com/v2/tickers?symbols=ALL', ct))))

        sleep(interval)
        counter += 1
        if counter % interval*10 == 0:
            print(str(counter))

    from_ticker_to_parquet()
    #read_parquet()


if __name__ == '__main__':
    main()