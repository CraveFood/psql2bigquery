import click

from psql2bigquery.tools import logging


logging.setup_logging()


@click.group()
@click.version_option()
def cli():
    """Welcome to PSQL to BigQuery CLI

    Command Line Interface for moving PostgreSQL data to BigQuery

    You can start with:
    psql2bigquery run --help
    """
