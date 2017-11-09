import json

import pytest
from doublex import assert_that
from hamcrest import equal_to

from target_datadotworld.utils import to_jsonlines_chunks, to_jsonlines, \
    to_stream_id, to_dataset_id


def test_to_jsonline():
    records = [{'id': x} for x in range(10)]
    jsonlines = to_jsonlines(records)
    assert_that([json.loads(line) for line in jsonlines.split('\n')],
                equal_to(records))


def test_to_jsonline_chunks():
    records = [{'id': x} for x in range(10)]
    chunks = list(to_jsonlines_chunks(records, 3))

    assert_that(len(chunks), equal_to(4))
    assert_that(len('\n'.join(chunks).split('\n')), equal_to(10))


@pytest.mark.parametrize('text,streamid', [
    ('a'*100, 'a'*95),
    ('a1!_b@2_c3', 'a-1-b-2-c-3')
])
def test_to_streamid(text, streamid):
    assert_that(to_stream_id(text), equal_to(streamid))


@pytest.mark.parametrize('text,datasetid', [
    ('a'*100, 'a'*95),
    ('a1!_b@2_c3', 'a-1-b-2-c-3')
])
def test_to_dataset_id(text, datasetid):
    assert_that(to_dataset_id(text), equal_to(datasetid))
