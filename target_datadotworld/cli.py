import json

import click
import logging

from target_datadotworld import logger
from target_datadotworld.exceptions import Error
from target_datadotworld.target import TargetDataDotWorld


@click.command()
@click.option('-c', '--config', required=True,
              type=click.File('r'), help='Path to config file')
@click.option('--debug', is_flag='True')
@click.option('--file', type=click.File('r'))
@click.pass_context
def cli(ctx, config, debug, file):
    if debug:
        logger.setLevel(logging.DEBUG)

    try:
        config_obj = json.load(config)

        target = TargetDataDotWorld(config_obj)
        input = file or click.get_text_stream('stdin')
        state = target.process_lines(input)

        if state is not None:
            line = json.dumps(state)
            logger.debug('Emitting state {}'.format(line))
            click.echo(line)
    except Error as e:
        logger.fatal(e.message)
        ctx.exit(1)
    except:
        logger.fatal('Unexpected failure', exc_info=True)
        ctx.exit(1)

    logger.debug('Exiting normally')

if __name__ == '__main__':
    cli()