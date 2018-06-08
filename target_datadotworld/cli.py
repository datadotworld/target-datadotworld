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
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor

import click

from target_datadotworld import logger
from target_datadotworld.exceptions import Error
from target_datadotworld.singer_analytics import send_usage_stats
from target_datadotworld.target import TargetDataDotWorld


async def echo_states(state_gen):
    async for state in state_gen:
        if state is not None:
            logger.debug('Emitting state {}'.format(state))
            click.echo(json.dumps(state))


@click.command()
@click.option('-c', '--config', required=True,
              type=click.File('r'),
              help='Path to config file')
@click.option('--debug', is_flag='True', default=False)
@click.option('--file', type=click.File('r'),
              help='Path to file, if not using stdin')
@click.pass_context
def cli(ctx, config, debug, file):
    loop = asyncio.get_event_loop()

    if debug:
        logger.setLevel(logging.DEBUG)
        warnings.simplefilter('default')
        loop.set_debug(debug)

    # noinspection PyBroadException
    try:
        config_obj = json.load(config)

        if not config_obj.get('disable_collection', False):
            logger.info('Sending version information to singer.io. ' +
                        'To disable sending anonymous usage data, set ' +
                        'the config parameter "disable_collection" to true')
            loop.run_in_executor(ThreadPoolExecutor(max_workers=1),
                                 send_usage_stats)

        target = TargetDataDotWorld(config_obj)
        data_file = file or click.get_text_stream('stdin')

        loop.run_until_complete(echo_states(target.process_lines(data_file)))

    except Error as e:
        logger.fatal(e.message)
        ctx.exit(1)
    except Exception:
        logger.fatal('Unexpected failure', exc_info=True)
        ctx.exit(1)
    finally:
        loop.close()

    logger.info('Exiting normally')


if __name__ == '__main__':
    cli()
