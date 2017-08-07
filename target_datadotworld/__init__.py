"""
implements a singer (http://singer.io) target for writing to data.world datasets
"""
from io import TextIOWrapper
from sys import stdin, stdout
from json import loads, dumps
from datetime import datetime
import singer
from singer import utils
from target_datadotworld.files import CsvStreamDictWriter, JsonlStreamDictWriter
from datadotworld.util import parse_dataset_key
from datadotworld.datadotworld import DataDotWorld
from datadotworld.config import DefaultConfig


LOGGER = singer.get_logger()


class DictConfig(DefaultConfig):
    """
    simple data.world configuration class to handle passing the auth_token in a dict
    """
    def __init__(self, config):
        super(DictConfig, self).__init__()
        self._auth_token = config["auth_token"]


def validate_args(args):
    """
    validate the target's arguments and return the ownerid/datasetid
    :param args: the singer target's configuration dict
    :return: ownerid, datasetid, output_format, and timestamp_files
    """
    (ownerid, datasetid) = parse_dataset_key(args.config.get('dataset_key'))
    output_format = args.config.get('output_format', 'csv')
    if output_format not in {'csv', 'jsonl'}:
        raise Exception(
            "output_format must be either 'csv' or 'jsonl'")
    timestamp_files = args.config.get('timestamp_files', False)
    if not isinstance(timestamp_files, bool):
        raise Exception(
            "type of timestamp_files config argument should be bool")
    return ownerid, datasetid, output_format, timestamp_files


def process(lines, ownerid, datasetid, output_format, timestamp_files, config):
    """
    process the stream of input lines to the singer target
    :param lines: the stream of input lines
    :param ownerid: the owner id of the dataset to upload to
    :param datasetid: the dataset id of the dataset to upload to
    :param output_format: the type of file to write ('csv' or 'jsonl')
    :param timestamp_files: true IFF we should append a timestamp to
    make each file unique
    :param config: the data.world configuration object
    :return: the state and the streams extracted from the input lines
    """
    state = None
    writers = {}
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    ddw = DataDotWorld(config=config)

    try:
        for line_string in lines:
            line = loads(line_string)
            if line['type'] == 'RECORD':
                stream = line['stream']
                writers[stream].write_row(line['record'])
            elif line['type'] == 'SCHEMA':
                stream, schema = line['stream'], line['schema']
                filename = "{}-{}.{}".format(stream, timestamp, output_format) \
                    if timestamp_files else "{}.{}".format(stream, output_format)
                LOGGER.info("writing stream [%s] to file [%s] in dataset "
                            "[%s/%s]",
                            stream, filename, ownerid, datasetid)
                if output_format == 'csv':
                    f = ddw.open_remote_file(
                        dataset_key="{}/{}".format(ownerid, datasetid),
                        file_name=filename)
                    f.open()
                    writers[stream] = CsvStreamDictWriter(
                        ownerid, datasetid, filename, f,
                        fieldnames=schema['properties'].keys())
                elif output_format == 'jsonl':
                    f = ddw.open_remote_file(
                        dataset_key="{}/{}".format(ownerid, datasetid),
                        file_name=filename)
                    f.open()
                    writers[stream] = JsonlStreamDictWriter(
                        ownerid, datasetid, filename, f)
            elif line['state'] == 'STATE':
                state = line['value']
    finally:
        for stream, writer in writers.items():
            writer.close()
            LOGGER.info("wrote %s rows from stream [%s] to file [%s] in "
                        "dataset [%s/%s]",
                        writer.rows_written(), stream, writer.filename(),
                        writer.ownerid(), writer.datasetid())

    return state


def main():
    """
    the main method for the target-datadotworld singer target
    :return:
    """
    args = utils.parse_args(['auth_token', 'dataset_key'])
    ownerid, datasetid, output_format, timestamp_files = validate_args(args)

    config = DictConfig(args.config)

    state = process(
        TextIOWrapper(stdin.buffer, encoding="utf-8"),
        ownerid,
        datasetid,
        output_format,
        timestamp_files,
        config
    )

    if state:
        stdout.write("{}\n".format(dumps(state)))
