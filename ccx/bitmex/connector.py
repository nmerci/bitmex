from api_keys import API_KEY, API_SECRET

import bitmex
from bitmex_websocket import BitMEXWebsocket

import pandas as pd

from urllib.request import urlretrieve
from datetime import datetime, timedelta
import threading
from time import sleep
import os
import logging


LOCAL_DATA_PATH = "data/bitmex"

logging.basicConfig(level=logging.INFO)


class Connector(object):
    def __init__(self, symbol: str = "XBTUSD"):
        self._symbol = symbol

        self._rest_api = bitmex.bitmex(test=True, api_key=API_KEY, api_secret=API_SECRET)
        self._ws_api = BitMEXWebsocket(endpoint="https://testnet.bitmex.com/api/v1",
                                       symbol=self._symbol, api_key=API_KEY, api_secret=API_SECRET)

        self._static_data = pd.DataFrame()
        self._data = []

    def run(self, start_time: datetime):
        # download data from S3
        self._download_data_from_s3()

        # get static data
        historical_data = self._get_data_from_local(self._symbol, start_time)
        recent_data = self._get_data_from_rest_api(historical_data["timestamp"].max())

        self._static_data = pd.concat([historical_data, recent_data])

        # get realtime data
        threading.Thread(target=self._get_data_from_ws_api).start()

    def get_static_data(self):
        return self._static_data

    def get_data(self):
        return self._data

    @staticmethod
    def _download_data_from_s3(start: datetime = datetime(2014, 11, 22)):
        URL = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/"

        if not os.path.exists(LOCAL_DATA_PATH):
            os.makedirs(LOCAL_DATA_PATH)

        for dt in [start + timedelta(days=d) for d in range((datetime.now() - start).days)]:
            file_name = f"{str(dt.year)}{str(dt.month).zfill(2)}{str(dt.day).zfill(2)}.csv.gz"
            file_path = os.path.join(LOCAL_DATA_PATH, file_name)

            if os.path.exists(file_path):
                continue

            try:
                logging.info(f"Downloading {file_name}")
                urlretrieve(os.path.join(URL, file_name), file_path)
            except Exception as e:
                logging.exception(e)
                break

    @staticmethod
    def _get_data_from_local(symbol: str, start_time: datetime):
        result = []

        for dt in [start_time + timedelta(days=d) for d in range((datetime.now() - start_time).days)]:
            file_name = f"{str(dt.year)}{str(dt.month).zfill(2)}{str(dt.day).zfill(2)}.csv.gz"
            file_path = os.path.join(LOCAL_DATA_PATH, file_name)

            if os.path.exists(file_path):
                df = pd.read_csv(file_path, usecols=["timestamp", "symbol", "side", "size", "price", "trdMatchID"])
                df = df[df["symbol"] == symbol]

                result.append(df)
            else:
                break

        result = pd.concat(result)
        result["timestamp"] = pd.to_datetime(result["timestamp"], format="%Y-%m-%dD%H:%M:%S.%f")

        return result

    def _get_data_from_rest_api(self, start_time: datetime):
        delay = 0.5
        count = 500
        start = 0

        result = []

        while True:
            try:
                query = self._rest_api.Trade.Trade_get(symbol=self._symbol, startTime=start_time, start=start,
                                                       count=count, reverse=False)
                response = query.result()[0]

                logging.info(f"Last timestamp: {response[-1]['timestamp']}")

                result.extend(response)
            except Exception as e:
                logging.error(e)
                break

            if len(response) < count:
                break

            start += count
            sleep(delay)

        result = pd.DataFrame(result)
        result = result[["timestamp", "symbol", "side", "size", "price", "trdMatchID"]]
        result["timestamp"] = pd.to_datetime(result["timestamp"], format="%Y-%m-%dD%H:%M:%S.%f")

        return result

    def _get_data_from_ws_api(self):
        delay = 0.2

        ids = set(self._static_data["trdMatchID"].tail(500))

        while self._ws_api.ws.sock.connected:
            trades = self._ws_api.recent_trades()
            trades = [t for t in trades if t["trdMatchID"] not in ids]

            ids.update(t["trdMatchID"] for t in trades)
            self._data.extend(trades)

            sleep(delay)

        raise ConnectionError()


if __name__ == '__main__':
    con = Connector()
    con.run(datetime(2019, 6, 30))

    x = con.get_data()
    sleep(10)
    x = con.get_data()
    sleep(10)
    x = con.get_data()
