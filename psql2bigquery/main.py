# Requires the following environment variables
# DB_HOST : PostgreSQL IP address
# DB_USER : PostgreSQL username
# DB_PASSWORD : PostgreSQL username password
# DB_PORT : PostgreSQL database port (defaults to 5432)
# DB_NAME : PostgreSQL database name
# SCHEMA : PostgreSQL database schema (defaults to public)
# GCP_PROJECT : Google Cloud project name
# GCP_DATASET : BigQuery dataset name
# GCP_CREDENTIAL_PATH : path to Google Cloud credentials (defaults to ./credentials.json)

import os
from pathlib import Path

import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
import psycopg2
from google.oauth2 import service_account

# Setup Sentry Logging
SENTRY_DSN = os.environ.get("SENTRY_DSN")
if SENTRY_DSN:
    sentry_logging = LoggingIntegration(
        level=logging.INFO,
        event_level=logging.ERROR,
    )
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[sentry_logging]
    )

# Setup Variables
IGNORE_TABLES = (
    "order_historicalmasterorder",
    "order_historicalorder",
    "order_historicalorderitem",
    "notification_inappnotification",
    "watson_searchentry",
    "spatial_ref_sys",
)
IGNORE_PREFIXES = (
    "django_",
    "square_",
    "pg_",
)

SCHEMA: str = "public"
GCP_PROJECT = os.environ["GCP_PROJECT"]
GCP_DATASET = os.environ.get("GCP_DATASET", "api")
GCP_CREDENTIAL_PATH = os.environ.get("GCP_CREDENTIAL_PATH", "./credentials.json")

DELIMITER = "~"
QUOTE = '"'


class PSQL:
    _conn = None

    @classmethod
    def _connection(cls):
        if not cls._conn:
            cls._conn = psycopg2.connect(
                host=os.environ["DB_HOST"],
                database=os.environ["DB_NAME"],
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                port=os.environ.get("DB_PORT", "5432"),
            )
        return cls._conn

    @classmethod
    def _execute_query(cls, sql: str):
        cur = cls._connection().cursor()
        cur.execute(sql)
        cls._conn.commit()
        output = cur.fetchall()
        return output

    @classmethod
    def dump_table(cls, table_name: str):
        cur = cls._connection().cursor()

        file = Path(__file__).parent / "dump" / f"{table_name}.csv"
        file.parent.mkdir(exist_ok=True, parents=True)

        with file.open("w") as f:
            sql = (
                f"COPY (SELECT * FROM {table_name}) TO STDOUT "
                f"WITH (FORMAT CSV, HEADER TRUE, DELIMITER '{DELIMITER}', QUOTE '{QUOTE}', FORCE_QUOTE *);"
            )
            cur.copy_expert(sql=sql, file=f)
            cls._conn.commit()

        return file

    @classmethod
    def list_tables(cls):
        def _is_accepted(name):
            return name.lower() in IGNORE_TABLES or any(name.startswith(prefix) for prefix in IGNORE_PREFIXES)

        def _fetch_names(query):
            for row in cls._execute_query(sql=query):
                name = row[0]
                if not _is_accepted(name=name):
                    logging.debug(f"Ignoring: {name}")
                    continue
                yield name

        sql = f"SELECT tablename FROM pg_tables WHERE schemaname='{SCHEMA}'"
        yield from _fetch_names(query=sql)

        sql = f"select table_name from INFORMATION_SCHEMA.views WHERE table_schema='{SCHEMA}'"
        yield from _fetch_names(query=sql)

    @classmethod
    def close(cls):
        cls._connection().close()


class Parser:
    @classmethod
    def parse_output_as_rows(cls, output):
        return output


class BigQuery:
    _client = None

    @classmethod
    def client(cls) -> bigquery.Client:
        if not cls._client:
            credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIAL_PATH)
            cls._client = bigquery.Client(
                credentials=credentials,
            )
        return cls._client

    @classmethod
    def load_to_bigquery(cls, table_name: str, file_path):
        table_id = f"{GCP_PROJECT}.{GCP_DATASET}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=DELIMITER,
            quote_character=QUOTE,
            allow_quoted_newlines=True,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=10,
        )

        load_job = cls.client().load_table_from_file(
            file_obj=file_path.open("rb"),
            destination=table_id,
            job_config=job_config,
        )

        load_job.result()

        return table_id

    @classmethod
    def check_table(cls, table_id: str):
        table = cls.client().get_table(table_id)
        return table.num_rows


def main():
    for table_name in PSQL.list_tables():
        logging.info(f"[{table_name}]")
        file_path = PSQL.dump_table(table_name=table_name)
        try:
            table_id = BigQuery.load_to_bigquery(table_name=table_name, file_path=file_path)
            logging.info(f"\tPSQL ---> BIGQUERY [{table_name}]: {BigQuery.check_table(table_id=table_id)} rows")
            os.remove(file_path)
        except BadRequest as exc:
            logging.error(f"\tPSQL ---> BIGQUERY [{table_name}]: {exc.errors}")

    PSQL.close()


if __name__ == "__main__":
    main()
