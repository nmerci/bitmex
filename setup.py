#!/usr/bin/env python

from setuptools import setup


setup(name="cryptocurrency-exchange",
      version="0.0.1",
      description="Cryptocurrency exchange connector",
      author="Vlad Khizanov",
      author_email="vl.khizanov@gmail.com",
      url="https://github.com/nmerci/cryptocurrency-exchange",
      install_requires=[
          "bitmex",
          "bitmex-ws",
          "pandas",
          "tqdm"]
      )
