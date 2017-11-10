# data.world-py
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

import asyncio
from os import path

import pytest


@pytest.fixture(params=[5, 10, 15])
def records_generator(request):
    def records_generator_internal(size):
        for i in range(size):
            yield {'id': i}

    return lambda: records_generator_internal(request.param)


@pytest.fixture(params=[5, 10, 15])
def records_queue(request, event_loop):
    async def produce(n, queue):
        for i in range(n):
            await queue.put({'id': i})
        await queue.put(None)

    queue = asyncio.Queue(loop=event_loop)
    asyncio.ensure_future(produce(request.param, queue), loop=event_loop)

    return queue, list(records_generator(request)())


@pytest.fixture()
def test_files_path():
    root_dir = path.dirname(path.abspath(__file__))
    return path.join(root_dir, 'fixtures')
