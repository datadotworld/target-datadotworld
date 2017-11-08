#!/usr/bin/env python
import re
from os import path

from setuptools import setup, find_packages


def read(*paths):
    filename = path.join(path.abspath(path.dirname(__file__)), *paths)
    with open(filename) as f:
        return f.read()


def find_version(*paths):
    contents = read(*paths)
    match = re.search(r'^__version__ = [\'"]([^\'"]+)[\'"]', contents, re.M)
    if not match:
        raise RuntimeError('Unable to find version string.')
    return match.group(1)


setup(name='target-datadotworld',
      version='0.1.0',
      description='Singer.io target for data.world',
      author='data.world',
      url='https://data.world',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_datadotworld'],
      install_requires=[
          'backoff>=1.4.3,<2.0a',
          'click>=6.7,<7.0a',
          'datadotworld==1.4.1',
          'jsonschema>=2.6.0,<3.0a',
          'PyJWT>=1.5.3,<2.0a',
          'requests>=2.18.4,<3.0a',
          'singer-python==2.1.0',
      ],
      setup_requires=[
          'pytest-runner>=2.11,<3.0a',
      ],
      tests_require=[
          'doublex>=1.8.4,<2.0a',
          'pyhamcrest>=1.9.0,<2.0a',
          'responses>=0.8.1,<1.0a',
          'pytest>=3.2.3,<4.0a',
      ],
      entry_points={
          'console_scripts': [
              'target-datadotworld=target_datadotworld.cli:cli',
          ],
      })
