import json
from copy import copy

import jwt
import singer
from jsonschema import validate, ValidationError
from jwt import DecodeError

from target_datadotworld.api_client import ApiClient
from target_datadotworld.exceptions import NotFoundError, TokenError, \
    ConfigError
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

        self._logger = singer.get_logger()

    def process_lines(self, lines):

        state = None
        schemas = {}
        buffers = {}

        self._api_client.connection_check()

        try:
            self._api_client.get_dataset(
                self.config['default_owner'],
                to_dataset_id(self.config['dataset_title']))
        except NotFoundError:
            self._api_client.create_dataset(
                self.config['default_owner'],
                to_dataset_id(self.config['dataset_title']),
                title=self.config['dataset_title'],
                visibility=self.config['default_visibility'],
                license=self.config['default_license'])

        for line in lines:
            # TODO error handling
            try:
                msg = singer.parse_message(line)
            except json.decoder.JSONDecodeError:
                self._logger.error("Unable to parse:\n{}".format(line))
                raise

            if isinstance(msg, singer.RecordMessage):
                if msg.stream not in schemas:
                    raise Exception('A record for stream {} was encountered '
                                    'before a corresponding schema'.format(
                        msg.stream))
                schema = schemas[msg.stream]
                validate(msg.record, schema)

                if msg.stream not in buffers:
                    # TODO Convert in-memory buffers into queues
                    buffers[msg.stream] = []

                buffers[msg.stream].append(msg.record)
            elif isinstance(msg, singer.SchemaMessage):
                schemas[msg.stream] = msg.schema
                # TODO Add support for key_properties
            elif isinstance(msg, singer.StateMessage):
                for stream, buffer in buffers.items():
                    self._api_client.append_stream(
                        self.config['default_owner'],
                        to_dataset_id(self.config['dataset_title']),
                        to_stream_id(stream),
                        buffer,
                        10000)
                buffers = {}
                state = msg.value

        return state

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):

        try:
            validate(config, CONFIG_SCHEMA)
        except ValidationError as e:
            raise ConfigError('Invalid configuration ({})'.format(e.message))

        try:
            decoded_token = jwt.decode(config['api_token'], verify=False)
        except DecodeError:
            raise TokenError('Unable to decode API token')

        sub_parties = decoded_token['sub'].split(':')
        if len(sub_parties) < 2:
            raise TokenError('Invalid API token')

        self._config = copy(config)
        self._config['dataset_title'] = (config.get('dataset_title') or
                                         config.get('namespace'))
        self._config['default_owner'] = config.get(
            'default_owner', sub_parties[1])
        self._config['default_visibility'] = config.get(
            'default_visibility', 'PRIVATE')
        self._config['default_license'] = config.get('default_license')
