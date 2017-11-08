import json


def to_json_lines(records):
    json_lines = [json.dumps(r) for r in records]
    return '\n'.join(json_lines)