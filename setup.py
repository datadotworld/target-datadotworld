#!/usr/bin/env python

from setuptools import setup

setup(name='target-datadotworld',
      version='0.1.0',
      description='Singer.io target for data.world',
      author='data.world',
      url='https://data.world',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=[
          'target_datadotworld'
      ],
      install_requires=[
          'datadotworld==1.4.1',
          'singer-python==2.1.0',
      ],
      setup_requires=[
          'pytest-runner>=2.11,<3.0a',
      ],
      tests_require=[
          'pytest>=3.0.7,<4.0a'
      ],
      entry_points='''
          [console_scripts]
          target-datadotworld=target_datadotworld:main
      ''',
      packages=[
          'target_datadotworld',
          'target_datadotworld.files'
      ]
      )
