import asyncio
import json
import logging
import warnings

import click

from target_datadotworld import logger
from target_datadotworld.exceptions import Error
from target_datadotworld.target import TargetDataDotWorld


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

        target = TargetDataDotWorld(config_obj)
        data_file = file or click.get_text_stream('stdin')

        future = asyncio.ensure_future(target.process_lines(data_file))
        loop.run_until_complete(future)

        if future.result() is not None:
            line = json.dumps(future.result())
            logger.debug('Emitting state {}'.format(line))
            click.echo(line)
    except Error as e:
        logger.fatal(e.message)
        ctx.exit(1)
    except:
        logger.fatal('Unexpected failure', exc_info=True)
        ctx.exit(1)
    finally:
        loop.close()

    logger.info('Exiting normally')


if __name__ == '__main__':
    cli()
