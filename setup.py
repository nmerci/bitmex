#!/usr/bin/env python

from setuptools import setup


setup(name="bitmex",
      version="0.0.1",
      description="BitMEX API client.",
      author="Vlad Khizanov",
      author_email="vl.khizanov@gmail.com",
      url="https://github.com/nmerci/bitmex",
      install_requires=[
          "bitmex",
          "pandas",
          "tqdm"]
      )
