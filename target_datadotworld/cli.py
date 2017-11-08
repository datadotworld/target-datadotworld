import json

import click

from target_datadotworld.target import TargetDataDotWorld


@click.command()
@click.option('--config', type=click.File('r'), help='Path to config file')
def cli(config):
    config_obj = json.load(config)

    target = TargetDataDotWorld(config_obj)
    input = click.get_text_stream('stdin')
    state = target.process_lines(input)

    if state is not None:
        click.echo(json.dumps(state))

if __name__ == '__main__':
    cli()
