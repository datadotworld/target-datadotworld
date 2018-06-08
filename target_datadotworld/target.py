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
from singer import metrics, utils
from target_datadotworld import logger
from target_datadotworld.api_client import ApiClient
from target_datadotworld.exceptions import NotFoundError, TokenError, \
    ConfigError, MissingSchemaError, InvalidRecordError, \
    UnparseableMessageError, InvalidDatasetStateError
from target_datadotworld.utils import to_stream_id

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
            'pattern': '^[a-z0-9](?:-(?!-)|[a-z0-9]){1,93}[a-z0-9]$'
        },
        'dataset_owner': {
            'description': 'Account for new dataset created, '
                           'if not the owner of the token',
            'type': 'string',
            'pattern': '^[a-z0-9](?:-(?!-)|[a-z0-9]){1,29}[a-z0-9]$'
        },
        'disable_collection': {
            'description': 'If False, disables Singer usage data collection',
            'type': 'boolean'
        }
    },
    'required': ['api_token', 'dataset_id']
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
        api = self._api_client

        schemas = {}
        active_versions = {}

        queues = {}
        consumers = {}

        logger.info('Checking network connectivity')
        api.connection_check()  # Fail fast

        logger.info('Ensuring dataset exists and is in good state')
        await self._fix_dataset()

        with metrics.record_counter() as counter:
            for line in lines:
                try:
                    msg = singer.parse_message(line)
                except (json.JSONDecodeError, simplejson.JSONDecodeError) as e:
                    raise UnparseableMessageError(line, str(e))

                if isinstance(msg, singer.RecordMessage):
                    await self._handle_record_msg(
                        msg, schemas, active_versions, loop, queues, consumers)
                    counter.increment()
                    logger.debug('Line #{} in {} queued for upload'.format(
                        counter.value, msg.stream))
                elif isinstance(msg, singer.SchemaMessage):
                    logger.info('Schema found for {}'.format(msg.stream))
                    schemas[msg.stream] = await self._handle_schema_msg(msg)
                elif isinstance(msg, singer.StateMessage):
                    logger.info('State message found: {}'.format(msg.value))
                    state = await self._handle_state_msg(msg, queues,
                                                         consumers)
                    queues = {}
                    yield state
                elif isinstance(msg, singer.ActivateVersionMessage):
                    logger.info('Version message found: {}/{}'.format(
                        msg.stream, msg.version))

                    current_version = active_versions.get(msg.stream)
                    active_version = await self._handle_active_version_msg(
                        msg, current_version, api)
                    active_versions[msg.stream] = active_version
                else:
                    logger.warn('Unrecognized message ({})'.format(msg))

        await TargetDataDotWorld._drain_queues(queues, consumers)
        self._api_client.sync(self.config['dataset_owner'],
                              self.config['dataset_id'])

    async def _fix_dataset(self):
        try:
            dataset = self._api_client.get_dataset(
                self.config['dataset_owner'],
                self.config['dataset_id'])

            if dataset.get('status') != 'LOADED':
                raise InvalidDatasetStateError(
                    self.config['dataset_owner'],
                    self.config['dataset_id'])
        except NotFoundError:
            logger.info('Creating new dataset {}/{}'.format(
                self.config['dataset_owner'],
                self.config['dataset_id']))
            self._api_client.create_dataset(
                self.config['dataset_owner'],
                self.config['dataset_id'],
                title=self.config['dataset_id'],
                visibility='PRIVATE')

    async def _handle_active_version_msg(self, msg, current_version, api):
        if current_version is None:
            current_version = api.get_current_version(
                self.config['dataset_owner'],
                self.config['dataset_id'],
                to_stream_id(msg.stream))
        if str(msg.version) != str(current_version):
            api.truncate_stream_records(
                self.config['dataset_owner'],
                self.config['dataset_id'],
                to_stream_id(msg.stream))
        return msg.version

    async def _handle_record_msg(self, msg, schemas, active_versions,
                                 loop, queues, consumers):
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
        record = msg.record
        record.update({
            'singer_timestamp': utils.strftime(
                msg.time_extracted or utils.now()),
            'singer_version': active_versions.get(msg.stream)
        })
        await queues[msg.stream].put(record)

    async def _handle_schema_msg(self, msg):
        if (msg.key_properties is not None and
                len(msg.key_properties) > 0):

            bookmark_properties = msg.bookmark_properties
            if (bookmark_properties is None or
                    len(bookmark_properties) > 0):
                logger.warn(
                    'Found missing or multiple bookmark '
                    'properties for stream {} when data.world '
                    'requires a single field. Records will be '
                    'sorted in the order that they were '
                    'extracted.'.format(msg.stream))
                bookmark_properties = 'singer_timestamp'

            logger.info('Setting data.world schema {}/{}'.format(
                msg.key_properties, bookmark_properties))

            self._api_client.set_stream_schema(
                self.config['dataset_owner'],
                self.config['dataset_id'],
                to_stream_id(msg.stream),
                primaryKeyFields=msg.key_properties,
                sequenceField=bookmark_properties,
                updateMethod='TRUNCATE')

        return msg.schema

    async def _handle_state_msg(self, msg, queues, consumers):
        await TargetDataDotWorld._drain_queues(queues, consumers)
        return msg.value

    @staticmethod
    async def _drain_queues(queues, consumers):
        for q in queues:
            # Mark the end of each queue
            await queues[q].put(None)
            # Wait until all items in the queue are consumed
            await queues[q].join()
            # Make sure consumers are done
            await consumers[q]

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
        self._config['dataset_id'] = config.get('dataset_id')
        self._config['dataset_owner'] = config.get(
            'dataset_owner', sub_parties[1])
