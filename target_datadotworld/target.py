import asyncio
import json
from copy import copy

import jwt
import simplejson
import singer
from jsonschema import validate, ValidationError, SchemaError
from jwt import DecodeError
from singer import metrics

from target_datadotworld import logger
from target_datadotworld.api_client import ApiClient
from target_datadotworld.exceptions import NotFoundError, TokenError, \
    ConfigError, MissingSchemaError, InvalidRecordError, \
    UnparseableMessageError, Error
from target_datadotworld.utils import to_dataset_id, to_stream_id

CONFIG_SCHEMA = config_schema = {
    "$schema": "http://json-schema.org/draft-06/schema#",
    "title": 'data.world target configuration',
    'description': 'Configuration format for '
                   'data.world\'s Singer target',
    'type': 'object',
    'properties': {
        'api_token': {
            'description': 'data.world API authorization token',
            'type': 'string'
        },
        'dataset_title': {
            'description': 'Target dataset title',
            'type': 'string',
            'minLength': 3,
            'maxLength': 60
        },
        'default_license': {
            'description': 'Default license for new datasets created',
            'type': 'string',
            'enum': ['Public Domain', 'PDDL', 'CC-0', 'CC-BY',
                     'ODC-BY', 'CC-BY-SA', 'ODC-ODbL', 'CC BY-NC',
                     'CC BY-NC-SA', 'Other']
        },
        'default_visibility': {
            'description': 'Default visibility '
                           'for new datasets created',
            'type': 'string',
            'enum': ['OPEN', 'PRIVATE']
        },
        'default_owner': {
            'description': 'Default account for new datasets created, '
                           'if not the owner of the token',
            'type': 'string',
            'pattern': '[a-z0-9](?:-(?!-)|[a-z0-9]){1,29}[a-z0-9]'
        },
        'namespace': {
            'description': 'Target dataset title (reserved for Stitch)',
            'type': 'string',
            'minLength': 3,
            'maxLength': 60
        }
    },
    'oneOf': [
        {'required': ['api_token', 'namespace']},
        {'required': ['api_token', 'dataset_title']}
    ]
}


class TargetDataDotWorld(object):
    def __init__(self, config, **kwargs):
        self.config = config
        self._api_client = kwargs.get('api_client',
                                      ApiClient(self.config['api_token']))
        self._batch_size = kwargs.get('batch_size', 1000)

    async def process_lines(self, lines, loop=None):

        loop = loop or asyncio.get_event_loop()

        state = None
        schemas = {}
        queues = {}
        consumers = {}

        logger.info('Checking network connectivity')
        self._api_client.connection_check()

        try:
            self._api_client.get_dataset(
                self.config['default_owner'],
                to_dataset_id(self.config['dataset_title']))
        except NotFoundError:
            logger.info('Creating new dataset {}/{}'.format(
                self.config['default_owner'],
                to_dataset_id(self.config['dataset_title'])))
            self._api_client.create_dataset(
                self.config['default_owner'],
                to_dataset_id(self.config['dataset_title']),
                title=self.config['dataset_title'],
                visibility=self.config['default_visibility'],
                license=self.config['default_license'])

        with metrics.record_counter() as counter:
            for line in lines:
                try:
                    msg = singer.parse_message(line)
                except (json.JSONDecodeError, simplejson.JSONDecodeError) as e:
                    raise UnparseableMessageError(line, str(e))

                if isinstance(msg, singer.RecordMessage):
                    if msg.stream not in schemas:
                        raise MissingSchemaError(msg.stream)
                    schema = schemas[msg.stream]

                    try:
                        validate(msg.record, schema)
                    except (SchemaError, ValidationError) as e:
                        raise InvalidRecordError(msg.stream, e.message)

                    if msg.stream not in queues:
                        queue = asyncio.Queue(maxsize=self._batch_size)
                        queues[msg.stream] = queue
                        consumers[msg.stream] = asyncio.ensure_future(
                            self._api_client.append_stream_chunked(
                                self.config['default_owner'],
                                to_dataset_id(self.config['dataset_title']),
                                to_stream_id(msg.stream),
                                queue,
                                self._batch_size), loop=loop)

                    await queues[msg.stream].put(msg.record)
                    counter.increment()
                    logger.debug('Line #{} in {} queued for upload'.format(
                        counter.value, msg.stream))
                elif isinstance(msg, singer.SchemaMessage):
                    logger.info('Schema found for {}'.format(msg.stream))
                    schemas[msg.stream] = msg.schema
                    # TODO Add support for key_properties
                elif isinstance(msg, singer.StateMessage):
                    logger.info('State message found: {}'.format(msg.value))
                    state = msg.value
                else:
                    raise Error('Unrecognized message'.format(msg))

        for q in queues:
            await queues[q].put(None)
            await queues[q].join()
            await consumers[q]

        return state

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):

        try:
            validate(config, CONFIG_SCHEMA)
        except ValidationError as e:
            raise ConfigError(cause=e.message)

        try:
            decoded_token = jwt.decode(config['api_token'], verify=False)
        except DecodeError:
            raise TokenError()

        sub_parties = decoded_token['sub'].split(':')
        if len(sub_parties) < 2:
            raise TokenError()

        self._config = copy(config)
        self._config['dataset_title'] = (config.get('dataset_title') or
                                         config.get('namespace'))
        self._config['default_owner'] = config.get(
            'default_owner', sub_parties[1])
        self._config['default_visibility'] = config.get(
            'default_visibility', 'PRIVATE')
        self._config['default_license'] = config.get('default_license')
