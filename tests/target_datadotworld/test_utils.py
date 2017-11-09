import json
from functools import reduce
from math import ceil

import pytest
from doublex import assert_that
from hamcrest import equal_to

from target_datadotworld.utils import to_chunks, to_jsonlines, \
    to_stream_id, to_dataset_id


def test_to_jsonline():
    records = [{'id': x} for x in range(10)]
    jsonlines = to_jsonlines(records)
    assert_that([json.loads(line) for line in jsonlines.split('\n')],
                equal_to(records))


@pytest.mark.asyncio
async def test_to_chunks(records_queue):
    queue, records = records_queue
    chunks = []
    async for chunk in to_chunks(queue, 3):
        chunks.append(chunk)

    assert_that(len(chunks), equal_to(ceil(len(records) / 3)))
    assert_that(reduce(lambda x, y: x + len(y), chunks, 0),
                equal_to(len(records)))


@pytest.mark.parametrize('text,streamid', [
    ('a' * 100, 'a' * 95),
    ('a1!_b@2_c3', 'a-1-b-2-c-3')
])
def test_to_streamid(text, streamid):
    assert_that(to_stream_id(text), equal_to(streamid))


@pytest.mark.parametrize('text,datasetid', [
    ('a' * 100, 'a' * 95),
    ('a1!_b@2_c3', 'a-1-b-2-c-3')
])
def test_to_dataset_id(text, datasetid):
    assert_that(to_dataset_id(text), equal_to(datasetid))
