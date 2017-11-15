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

import json
import re


def to_jsonlines(records):
    """Convert objects into JSON lines

    :param records: Objects to be converted into JSON lines
    :type records: iterable

    :return: A JSON lines string
    :rtype: str
    """
    json_lines = [json.dumps(r) for r in records]
    return '\n'.join(json_lines)


async def to_chunks(queue, chunk_size):
    """Asynchronously convert objects into chunks of JSON lines

    This async generator will consume objects in a queue and emit chunks

    :param queue: Queue with objects
    :type queue: asyncio.Queue
    :param chunk_size: Chunk or batch size
    :type chunk_size: int

    :returns: Chunks of JSON line strings
    :rtype: str
    """
    lines = []
    while True:
        line = await queue.get()

        if line is None:
            if len(lines) > 0:
                yield lines
            queue.task_done()
            break

        lines.append(line)

        if len(lines) == chunk_size:
            yield lines
            lines = []

        queue.task_done()


def to_stream_id(stream_name):
    """Convert any string into a valid stream ID"""
    return kebab_case(stream_name)[0:95]


def to_dataset_id(dataset_title):
    """Convert any string into a valid dataset ID"""
    return kebab_case(dataset_title)[0:95]


# lodash/pydash style kebab_case implementation

UPPER = '[A-Z\\xC0-\\xD6\\xD8-\\xDE]'
LOWER = '[a-z\\xDf-\\xF6\\xF8-\\xFF]+'
RE_WORDS = ('/{upper}+(?={upper}{lower})|{upper}?{lower}|{upper}+|[0-9]+/g'
            .format(upper=UPPER, lower=LOWER))


def js_to_py_re_find(reg_exp):
    """Return Python regular expression matching function based on Javascript
    style regexp.
    """
    pattern, options = reg_exp[1:].rsplit('/', 1)
    flags = re.I if 'i' in options else 0

    def find(text):
        if 'g' in options:
            results = re.findall(pattern, text, flags=flags)
        else:
            results = re.search(pattern, text, flags=flags)

            if results:
                results = [results.group()]
            else:
                results = []

        return results

    return find


def kebab_case(text):
    """Converts `text` to kebab case (a.k.a. spinal case).

    Args:
        text (str): String to convert.

    Returns:
        str: String converted to kebab case.

    Example:

        >>> kebab_case('a b c_d-e!f')
        'a-b-c-d-e-f'

    .. versionadded:: 1.1.0
    """
    return '-'.join(word.lower()
                    for word in js_to_py_re_find(RE_WORDS)(text) if word)
