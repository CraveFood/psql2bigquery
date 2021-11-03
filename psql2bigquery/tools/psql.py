from dataclasses import dataclass

import logging
from pathlib import Path
from typing import ClassVar, Iterable, Generator

import psycopg2

from psql2bigquery.tools.config import SourceConfig, DumpConfig


@dataclass
class PostgreSQLClient:
    source: SourceConfig
    dump_config: DumpConfig

    _connection: ClassVar = None

    def __post_init__(self):
        if not self._connection:
            self._connection = psycopg2.connect(
                host=self.source.hostname,
                database=self.source.database_name,
                user=self.source.user,
                password=self.source.password,
                port=self.source.port,
            )
        return self._connection

    def _execute_query(self, sql: str) -> Iterable:
        cur = self._connection.cursor()
        cur.execute(sql)
        self._connection.commit()
        output = cur.fetchall()
        return output

    def dump_table(self, table_name: str) -> Path:
        cur = self._connection.cursor()

        file = self.dump_config.dump_directory / f"{table_name}.csv"
        file.parent.mkdir(exist_ok=True, parents=True)

        delimiter = self.dump_config.delimiter
        quote = self.dump_config.quote

        with file.open("w") as sql_file:
            sql = (
                f"COPY (SELECT * FROM {table_name}) TO STDOUT "
                f"WITH (FORMAT CSV, HEADER TRUE, DELIMITER '{delimiter}', QUOTE '{quote}', FORCE_QUOTE *);"
            )
            cur.copy_expert(sql=sql, file=sql_file)
            self._connection.commit()

        return file

    def list_tables(self) -> Generator[str, None, None]:
        def _is_accepted(name):
            cleaned_name = name.lower()
            if self.dump_config.include_tables:
                return cleaned_name in self.dump_config.include_tables

            return cleaned_name not in self.dump_config.skip_tables or any(
                cleaned_name.startswith(prefix) for prefix in self.dump_config.skip_tables_prefix
            )

        def _fetch_names(query):
            for row in self._execute_query(sql=query):
                name = row[0]
                if not _is_accepted(name=name):
                    logging.debug(f"Ignoring: {name}")
                    continue
                yield name

        schema = self.source.database_schema
        sql = f"SELECT tablename FROM pg_tables WHERE schemaname='{schema}'"
        yield from _fetch_names(query=sql)

        sql = f"select table_name from INFORMATION_SCHEMA.views WHERE table_schema='{schema}'"
        yield from _fetch_names(query=sql)

    def get_columns(self, table_name):
        sql = (
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name='{table_name}' "
            "ORDER BY ordinal_position ASC;"
        )

        columns = {}
        for row in self._execute_query(sql=sql):
            columns[row[0]] = row[1]
        return columns

    def close(self) -> None:
        self._connection.close()
