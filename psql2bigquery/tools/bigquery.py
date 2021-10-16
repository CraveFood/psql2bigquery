from dataclasses import dataclass
from pathlib import Path

from typing import ClassVar, Dict, Tuple, List

from google.cloud import bigquery
from google.oauth2 import service_account

from psql2bigquery.tools.config import TargetConfig, DumpConfig


@dataclass
class BigQueryClient:
    target: TargetConfig
    dump_config: DumpConfig

    _client: ClassVar[bigquery.Client] = None

    def __post_init__(self):
        if not self._client:
            credentials = service_account.Credentials.from_service_account_file(
                filename=str(self.target.credential_path)
            )
            self._client = bigquery.Client(
                credentials=credentials,
            )
        return self._client

    def load_to_bigquery(self, table_name: str, file_path: Path, schema: List[bigquery.SchemaField]) -> str:
        table_id = f"{self.target.project}.{self.target.dataset}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=self.dump_config.delimiter,
            quote_character=self.dump_config.quote,
            allow_quoted_newlines=True,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=0,
        )

        load_job = self._client.load_table_from_file(
            file_obj=file_path.open("rb"),
            destination=table_id,
            job_config=job_config,
        )

        load_job.result()

        return table_id

    def check_table(self, table_id: str) -> int:
        table = self._client.get_table(table_id)
        return table.num_rows

    @classmethod
    def build_schema(cls, psql_column_types: Dict[str, str]) -> List[bigquery.SchemaField]:
        schema = []
        for field_name, psql_type in psql_column_types.items():
            bq_type, mode = cls.convert_data_type(psql_type=psql_type)
            field = bigquery.SchemaField(name=field_name, field_type=bq_type, mode=mode)
            schema.append(field)
        return schema

    @classmethod
    def convert_data_type(cls, psql_type: str) -> Tuple[str, str]:
        # PSQL's ARRAY type should be handled as REPEATED mode
        # But we cannot insert arrays when using CSV files
        mode = "NULLABLE"

        # STRING is the default fallback due to its flexibility.
        data_type = SUPPORTED_TYPES.get(psql_type, "STRING")
        return data_type, mode


SUPPORTED_TYPES = {
    "jsonb": "STRING",
    "name": "STRING",
    "character varying": "STRING",
    "time without time zone": "TIME",
    "uuid": "STRING",
    "json": "STRING",
    "text": "STRING",
    "integer": "INTEGER",
    "smallint": "INTEGER",
    "numeric": "FLOAT",
    "timestamp with time zone": "TIMESTAMP",
    "double precision": "FLOAT",
    "bigint": "INTEGER",
    "boolean": "BOOLEAN",
    "date": "DATE",
}
