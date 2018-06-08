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
import requests
import target_datadotworld
from target_datadotworld import logger
from requests import HTTPError


def send_usage_stats():
    try:
        version = target_datadotworld.__version__
        resp = requests.get('http://collector.singer.io/i',
                            params={
                                'e': 'se',
                                'aid': 'singer',
                                'se_ca': 'target-datadotworld',
                                'se_ac': 'open',
                                'se_la': version,
                            }, timeout=0.5)
        resp.raise_for_status()
    except HTTPError:
        logger.debug('Collection request failed')
