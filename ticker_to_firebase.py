import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json
import requests
from time import time
from os import getcwd

GOOGLE_APPLICATION_CREDENTIALS = getcwd()+'/secret/firestore.json'


def ticker_to_json(arg=''):

    response = requests.get('https://poloniex.com/public?command=returnTicker')
    timestamp = int(time())
    output = {'timestamp': timestamp, 'exchange': 'poloniex', 'ticker_data': response.json()}
    return json.dumps(output)

def write_to_firebase():

    response = requests.get('https://poloniex.com/public?command=returnTicker')

    cred = credentials.Certificate(GOOGLE_APPLICATION_CREDENTIALS)
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    tickers_ref = db.collection(u'tickers')
    tickers = tickers_ref.stream()

    for ticker in tickers:
        print(tickers)

def main():
    write_to_firebase()
    #print(ticker_to_json())

if __name__ == '__main__':
    main()


