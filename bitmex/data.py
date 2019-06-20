import pandas as pd

from tqdm import tqdm

from urllib.request import urlretrieve
from datetime import datetime, timedelta
import os
import logging
import argparse

from typing import List


logging.basicConfig(level=logging.INFO)


def download_data(output_dir: str, start: datetime = datetime(year=2014, month=11, day=22), end: datetime = datetime.now()):
    URL = "https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    elif len(os.listdir(output_dir)) > 0:
        logging.warning(f"{output_dir} is not empty")

    for dt in tqdm([start + timedelta(days=x) for x in range((end - start).days)]):
        file_name = f"{str(dt.year)}{str(dt.month).zfill(2)}{str(dt.day).zfill(2)}.csv.gz"

        if os.path.exists(os.path.join(output_dir, file_name)):
            logging.warning(f"{file_name} is already exists")
            continue

        try:
            logging.info(f"Downloading {file_name}")
            urlretrieve(os.path.join(URL, file_name), os.path.join(output_dir, file_name))
        except Exception as e:
            logging.exception(e)


def aggregate_data(data_dir: str, output_dir: str, symbols: List[str] = None, from_date: datetime = None):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    elif len(os.listdir(output_dir)) > 0:
        logging.warning(f"{output_dir} is not empty")

    result = {}

    logging.info("Reading data...")
    for root, dirs, files in os.walk(data_dir):
        for f in tqdm(files):
            df = pd.read_csv(os.path.join(root, f), usecols=["timestamp", "symbol", "side", "size", "price"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%dD%H:%M:%S.%f")

            if from_date is not None:
                df = df[df["timestamp"] > from_date]

            for symbol, group in df.groupby("symbol"):
                if symbols is not None and symbol not in symbols:
                    continue

                if symbol not in result:
                    result[symbol] = []

                result[symbol].append(group)

    logging.info("Concatenating data...")
    for symbol in tqdm(result):
        pd.concat(result[symbol]).to_csv(os.path.join(output_dir, f"{symbol}.csv.gz"), index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparser_name")

    dl_parser = subparsers.add_parser("download")
    dl_parser.add_argument("-o", "--output", type=str)

    agg_parser = subparsers.add_parser("aggregate")
    agg_parser.add_argument("-i", "--input", type=str)
    agg_parser.add_argument("-o", "--output", type=str)
    agg_parser.add_argument("-s", "--symbols", type=str, nargs="+", default=None)

    args = parser.parse_args()

    if args.subparser_name == "download":
        download_data(args.output)
    elif args.subparser_name == "aggregate":
        aggregate_data(args.input, args.output, args.symbols)
    else:
        raise ValueError(f"Unknown option {args.subparser_name}")
