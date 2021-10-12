from dataclasses import dataclass
from pathlib import Path

from typing import ClassVar

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

    def load_to_bigquery(self, table_name: str, file_path: Path) -> str:
        table_id = f"{self.target.project}.{self.target.dataset}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=self.dump_config.delimiter,
            quote_character=self.dump_config.quote,
            allow_quoted_newlines=True,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=10,
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
