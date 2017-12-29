# target-datadotworld
# Copyright 2017 data.world, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# data.world, Inc.(http://data.world/).

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


setup(
    name='target-datadotworld',
    version=find_version('target_datadotworld', '__init__.py'),
    description='Singer target for data.world',
    long_description=read('README.rst'),
    url='http://github.com/datadotworld/target-datadotworld',
    author='data.world',
    author_email='help@data.world',
    license='Apache 2.0',
    keywords='data.world dataset singer target',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Scientific/Engineering :: Information Analysis',
    ],
    packages=find_packages(),
    install_requires=[
        'backoff>=1.3.0,<2.0a',
        'click>=6.7,<7.0a',
        'jsonschema>=2.6.0,<3.0a',
        'pyjwt>=1.5.3,<2.0a',
        'requests>=2.4.0,<3.0a',
        'singer-python>=5.0.4,<6.0a',
    ],
    setup_requires=[
        'pytest-runner>=2.11,<3.0a',
    ],
    tests_require=[
        'coverage>=4.4.2',
        'doublex>=1.8.4,<2.0a',
        'flake8>=2.6.0,<3.4.1a',
        'pyhamcrest>=1.9.0,<2.0a',
        'responses>=0.8.1,<1.0a',
        'pytest>=3.2.3,<4.0a',
        'pytest-asyncio>=0.8.0,<1.0a',
    ],
    entry_points={
        'console_scripts': [
            'target-datadotworld=target_datadotworld.cli:cli',
        ],
    })
