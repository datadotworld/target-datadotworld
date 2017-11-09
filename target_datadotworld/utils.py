import json
import re


def to_jsonlines(records):
    json_lines = [json.dumps(r) for r in records]
    return '\n'.join(json_lines)


async def to_chunks(queue, chunk_size):
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
    return kebab_case(stream_name)[0:95]


def to_dataset_id(dataset_title):
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
