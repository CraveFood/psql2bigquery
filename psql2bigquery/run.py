import logging
import os
from pathlib import Path
from typing import List

import click
from google.cloud.exceptions import BadRequest

from psql2bigquery.main import cli
from psql2bigquery.tools.bigquery import BigQueryClient
from psql2bigquery.tools.config import SourceConfig, TargetConfig, DumpConfig
from psql2bigquery.tools.psql import PostgreSQLClient


def _env(name, fallback=None):
    return os.environ.get(name, fallback)


@cli.command("run")
@click.option(
    "--db-host",
    type=str,
    default=_env("DB_HOST"),
    show_default=True,
    help="PSQL database hostname",
)
@click.option(
    "--db-port",
    type=int,
    default=_env("DB_PORT", 5432),
    show_default=True,
    help="PSQL database port",
)
@click.option(
    "--db-user",
    type=str,
    default=_env("DB_USER"),
    show_default=True,
    help="PSQL database username",
)
@click.option(
    "--db-password",
    type=str,
    default=_env("DB_PASSWORD"),
    hide_input=True,
    help="PSQL database password",
)
@click.option(
    "--db-schema",
    type=str,
    default=_env("DB_SCHEMA", "public"),
    show_default=True,
    help="PSQL database schema",
)
@click.option(
    "--db-name",
    type=str,
    default=_env("DB_NAME"),
    show_default=True,
    help="PSQL database name",
)
@click.option(
    "--gcp-project",
    type=str,
    default=_env("GCP_PROJECT"),
    show_default=True,
    help="BigQuery project ID",
)
@click.option(
    "--gcp-dataset",
    type=str,
    default=_env("GCP_DATASET"),
    show_default=True,
    help="BigQuery dataset name",
)
@click.option(
    "--gcp-credential-path",
    type=str,
    default=_env("GCP_CREDENTIAL_PATH"),
    show_default=True,
    help="BigQuery application credentials location",
)
@click.option(
    "--dump-dir",
    type=str,
    default=Path(__file__).parent / "dump",
    show_default=True,
    help="Temporary directory to store database dumps",
)
@click.option(
    "--delimiter",
    type=str,
    default=_env("CSV_DELIMITER", "~"),
    show_default=True,
    help="CSV delimiter character",
)
@click.option(
    "--quote",
    type=str,
    default=_env("CSV_QUOTE", '"'),
    show_default=True,
    help="CSV quote character",
)
@click.option(
    "--include",
    type=str,
    multiple=True,
    help="Tables to include",
)
@click.option(
    "--exclude",
    type=str,
    multiple=True,
    help="Tables to exclude",
)
@click.option("--exclude-prefix", type=str, multiple=True, help="Tables prefix to exclude")
def run(
    db_host: str,
    db_port: int,
    db_user: str,
    db_password: str,
    db_schema: str,
    db_name: str,
    gcp_project: str,
    gcp_dataset: str,
    gcp_credential_path: str,
    dump_dir: str,
    delimiter: str,
    quote: str,
    include: List[str],
    exclude: List[str],
    exclude_prefix: List[str],
):
    """Export PostgreSQL tables as BigQuery tables."""
    source = SourceConfig(
        hostname=db_host,
        user=db_user,
        password=db_password,
        database_name=db_name,
        database_schema=db_schema,
        port=db_port,
    )
    target = TargetConfig(
        project=gcp_project,
        dataset=gcp_dataset or db_name,
        credential_path=Path(gcp_credential_path),
    )
    dump_config = DumpConfig(
        dump_directory=Path(dump_dir),
        delimiter=delimiter,
        quote=quote,
        include_tables=include,
        skip_tables=exclude,
        skip_tables_prefix=exclude_prefix,
    )

    psql = PostgreSQLClient(
        source=source,
        dump_config=dump_config,
    )
    bigquery = BigQueryClient(
        target=target,
        dump_config=dump_config,
    )

    for table_name in psql.list_tables():
        logging.info(f"[{table_name}]")
        file_path = psql.dump_table(table_name=table_name)
        schema = bigquery.build_schema(psql_column_types=psql.get_columns(table_name=table_name))
        try:
            table_id = bigquery.load_to_bigquery(table_name=table_name, file_path=file_path, schema=schema)
            logging.info(f"\tPSQL ---> BIGQUERY [{table_name}]: {bigquery.check_table(table_id=table_id)} rows")
            os.remove(file_path)
        except BadRequest as exc:
            logging.error(f"\tPSQL ---> BIGQUERY [{table_name}]: {exc.errors}")

    psql.close()
