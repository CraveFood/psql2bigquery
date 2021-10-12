from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class SourceConfig:
    hostname: str
    user: str
    password: str
    database_name: str
    database_schema: str = "public"
    port: int = 5432


@dataclass
class DumpConfig:
    dump_directory: Path = Path(__file__).parent / "dump"
    delimiter: str = "~"
    quote: str = '"'
    include_tables: List[str] = field(default_factory=list)
    skip_tables: List[str] = field(default_factory=list)
    skip_tables_prefix: List[str] = field(default_factory=list)


@dataclass
class TargetConfig:
    project: str
    dataset: str
    credential_path: Path
