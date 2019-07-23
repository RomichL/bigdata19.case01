"""
Assignment 02
=============
The goal of this assignment is to implement synchronous scraping using standard python modules,
and compare the scraping speed to asynchronous mode.
Run this code with
    > fab run assignment02.py
"""

import requests
import os
from tqdm import tqdm
from yahoo import read_symbols, YAHOO_HTMLS

YAHOO_HTMLS_PATH = os.getcwd()+'"\"build\yahoo_html'
def scrape_descriptions_sync():
    """Scrape companies descriptions synchronously."""
    # TODO: Second assignment. Use https://docs.python.org/3/library/urllib.html
    symbols = read_symbols()
    YAHOO_HTMLS.mkdir(parents=True, exist_ok=True)
    progress_bar = tqdm(total=len(symbols))

    counter = 0
    for symbol in symbols:
        yahoo_url = 'https://finance.yahoo.com/quote/%s/profile?p=%s' % (symbol, symbol)
        page = requests.get(yahoo_url).content
        with open(YAHOO_HTMLS / f'{symbol}.html', 'wb') as f:
            f.write(page)
        progress_bar.update(1)
        counter += 1
        if counter > 100:
            return

def main():
    scrape_descriptions_sync()


if __name__ == '__main__':
    main()