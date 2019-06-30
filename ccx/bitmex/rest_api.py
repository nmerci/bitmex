from api_keys import API_KEY, API_SECRET

import bitmex

from datetime import datetime
from time import sleep
import logging


# get from local
# get from aws
# get from rest api
# get from ws api


def get_data(start_time: datetime = datetime(2019, 6, 27), symbol: str = "XBTUSD", delay: float = 0.2):
    clinet = bitmex.bitmex(test=True, api_key=API_KEY, api_secret=API_SECRET)

    trades = []

    count = 500
    start = 0

    while True:
        try:
            query = clinet.Trade.Trade_get(symbol=symbol, startTime=start_time, start=start, count=count, reverse=True)
            response = query.result()[0]
        except Exception as e:
            logging.error(e)
            continue

        trades.extend(response)

        if len(response) < count:
            break

        start += count
        sleep(delay)

    return trades


if __name__ == '__main__':
    get_data()
