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
          'backoff>=1.4.3,<2.0a',
          'datadotworld==1.4.1',
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
      entry_points='''
          [console_scripts]
          target-datadotworld=target_datadotworld:main
      ''',
      packages=[
          'target_datadotworld',
          'target_datadotworld.files'
      ]
      )
