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

#: Json schema specifying what is required in the config.json file
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
        'dataset_id': {
            'description': 'Target dataset id',
            'type': 'string',
            'pattern': '[a-z0-9](?:-(?!-)|[a-z0-9]){1,93}[a-z0-9]'
        },
        'dataset_title': {
            'description': 'Title for new dataset created',
            'type': 'string',
            'minLength': 3,
            'maxLength': 60
        },
        'dataset_license': {
            'description': 'License for new dataset created',
            'type': 'string',
            'enum': [
                'Public Domain', 'PDDL', 'CC-0', 'CC-BY',
                'ODC-BY', 'CC-BY-SA', 'ODC-ODbL', 'CC BY-NC',
                'CC BY-NC-SA', 'Other'
            ]
        },
        'dataset_visibility': {
            'description': 'Visibility for new dataset created',
            'type': 'string',
            'enum': ['OPEN', 'PRIVATE']
        },
        'dataset_owner': {
            'description': 'Account for new dataset created, '
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
        {'required': ['api_token', 'dataset_id']}
    ]
}


class TargetDataDotWorld(object):
    def __init__(self, config, **kwargs):
        """Singer target for data.world"""
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
        self._api_client.connection_check()  # Fail fast

        try:
            self._api_client.get_dataset(
                self.config['dataset_owner'],
                self.config['dataset_id'])
        except NotFoundError:
            logger.info('Creating new dataset {}/{}'.format(
                self.config['dataset_owner'],
                self.config['dataset_id']))
            self._api_client.create_dataset(
                self.config['dataset_owner'],
                self.config['dataset_id'],
                title=self.config['dataset_title'],
                visibility=self.config['dataset_visibility'],
                license=self.config['dataset_license'])

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
                        # Creates one queue per stream
                        queue = asyncio.Queue(maxsize=self._batch_size)
                        queues[msg.stream] = queue

                        # Schedules one consumer per queue
                        consumers[msg.stream] = asyncio.ensure_future(
                            self._api_client.append_stream_chunked(
                                self.config['dataset_owner'],
                                self.config['dataset_id'],
                                to_stream_id(msg.stream),
                                queue,
                                self._batch_size, loop=loop), loop=loop)

                    # Add record to queue
                    await queues[msg.stream].put(msg.record)
                    counter.increment()
                    logger.debug('Line #{} in {} queued for upload'.format(
                        counter.value, msg.stream))
                elif isinstance(msg, singer.SchemaMessage):
                    logger.info('Schema found for {}'.format(msg.stream))
                    schemas[msg.stream] = msg.schema
                    # Ignoring key_properties as the concept of primary keys
                    # currently does not apply to data.world
                elif isinstance(msg, singer.StateMessage):
                    logger.info('State message found: {}'.format(msg.value))
                    state = msg.value
                elif isinstance(msg, singer.ActivateVersionMessage):
                    logger.info('Version message found: {}/{}'.format(
                        msg.stream, msg.version))
                    # TODO Handle Active Version Messages (GH Issue #2)
                else:
                    raise Error('Unrecognized message'.format(msg))

        for q in queues:
            # Mark the end of each queue
            await queues[q].put(None)
            # Wait until all items in the queue are consumed
            await queues[q].join()
            # Make sure consumers are done
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
        self._config['dataset_id'] = (config.get('dataset_id') or
                                      to_dataset_id(config.get('namespace')))
        self._config['dataset_title'] = (config.get('dataset_title') or
                                         config.get('namespace') or
                                         config.get('dataset_id'))
        self._config['dataset_owner'] = config.get(
            'dataset_owner', sub_parties[1])
        self._config['dataset_visibility'] = config.get(
            'dataset_visibility', 'PRIVATE')
        self._config['dataset_license'] = config.get('dataset_license')
