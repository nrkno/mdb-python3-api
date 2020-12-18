#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(name='mdbclient',
      author='Kristian Rosenvold',
      author_email='kristian.rosenvold@nrk.no',
      description='Client library for MDB',
      version='1.5',
      url='https://github.com/nrkno/mdbclient-python3-api',
      packages=find_packages(),
      install_requires=[
          'aiohttp>=3.5.4',
          'aioamqp>=0.12.0'],
      classifiers=[
          'Programming Language :: Python :: 3.8'
      ]
      )
