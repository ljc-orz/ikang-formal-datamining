#!/usr/bin/env python3
"""Stream one Parquet file into a MySQL table with PyMySQL."""

from __future__ import annotations

import argparse
import math
import re
import sys
from pathlib import Path
from typing import Callable, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "datas_in_progress",
    "charset": "utf8mb4",
    "unix_socket": "/DaTa/mysql/mysql.sock",
    "local_infile": True,
    "autocommit": False,
}

DEFAULT_BATCH_SIZE = 5_000


def _connect_mysql():
    try:
        import pymysql
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyMySQL is not installed. Install it in the ikang environment with: "
            "conda run -n ikang python -m pip install PyMySQL"
        ) from exc
    return pymysql.connect(**DB_CONFIG)


def _quote_identifier(value: str) -> str:
    if not value or "\0" in value:
        raise ValueError("MySQL identifiers cannot be empty or contain NUL")
    return "`" + value.replace("`", "``") + "`"


def default_table_name(parquet_path: Path) -> str:
    table = re.sub(r"[^\w]+", "_", parquet_path.stem, flags=re.UNICODE).strip("_")
    return (table or "imported_parquet")[:64]


def _mysql_type(data_type: pa.DataType) -> str:
    if pa.types.is_dictionary(data_type):
        return _mysql_type(data_type.value_type)
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "LONGTEXT"
    if (
        pa.types.is_binary(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_fixed_size_binary(data_type)
    ):
        return "LONGBLOB"
    if pa.types.is_boolean(data_type):
        return "TINYINT(1)"
    if pa.types.is_int8(data_type):
        return "TINYINT"
    if pa.types.is_uint8(data_type):
        return "TINYINT UNSIGNED"
    if pa.types.is_int16(data_type):
        return "SMALLINT"
    if pa.types.is_uint16(data_type):
        return "SMALLINT UNSIGNED"
    if pa.types.is_int32(data_type):
        return "INT"
    if pa.types.is_uint32(data_type):
        return "INT UNSIGNED"
    if pa.types.is_int64(data_type):
        return "BIGINT"
    if pa.types.is_uint64(data_type):
        return "BIGINT UNSIGNED"
    if pa.types.is_float16(data_type) or pa.types.is_float32(data_type):
        return "FLOAT"
    if pa.types.is_float64(data_type):
        return "DOUBLE"
    if pa.types.is_decimal(data_type):
        if data_type.precision > 65 or data_type.scale > 30:
            raise TypeError(
                f"MySQL cannot represent Arrow decimal type {data_type} exactly"
            )
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    if pa.types.is_date(data_type):
        return "DATE"
    if pa.types.is_timestamp(data_type):
        return "DATETIME(6)"
    if pa.types.is_time(data_type):
        return "TIME(6)"
    if pa.types.is_null(data_type):
        return "LONGTEXT"
    raise TypeError(f"unsupported Arrow type for MySQL import: {data_type}")


def create_table_sql(table_name: str, schema: pa.Schema) -> str:
    if len(table_name) > 64:
        raise ValueError("MySQL table names cannot exceed 64 characters")
    if len(schema.names) != len(set(name.casefold() for name in schema.names)):
        raise ValueError("Parquet column names must be unique (case-insensitive)")

    definitions = []
    for field in schema:
        nullability = "" if field.nullable else " NOT NULL"
        definitions.append(
            f"{_quote_identifier(field.name)} {_mysql_type(field.type)}{nullability}"
        )
    if not definitions:
        raise ValueError("Parquet must contain at least one column")
    return (
        f"CREATE TABLE {_quote_identifier(table_name)} ("
        + ", ".join(definitions)
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )


def _insert_sql(table_name: str, column_names: Sequence[str]) -> str:
    columns = ", ".join(_quote_identifier(name) for name in column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    return (
        f"INSERT INTO {_quote_identifier(table_name)} ({columns}) "
        f"VALUES ({placeholders})"
    )


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s LIMIT 1",
        (DB_CONFIG["database"], table_name),
    )
    return cursor.fetchone() is not None


def _validate_append_columns(cursor, table_name: str, schema: pa.Schema) -> None:
    cursor.execute(f"SHOW COLUMNS FROM {_quote_identifier(table_name)}")
    existing_columns = [row[0] for row in cursor.fetchall()]
    if existing_columns != schema.names:
        raise ValueError(
            f"existing table {table_name!r} columns do not match Parquet; "
            f"MySQL={existing_columns}, Parquet={schema.names}"
        )


def _normalise_value(value):
    # PyMySQL rejects non-finite floats; represent them as SQL NULL, consistent
    # with missing numeric values from Parquet.
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def run_import(
    parquet_path: Path,
    table_name: Optional[str] = None,
    if_exists: str = "fail",
    batch_size: int = DEFAULT_BATCH_SIZE,
    connection_factory: Optional[Callable[[], object]] = None,
) -> int:
    parquet_path = Path(parquet_path).resolve()
    if not parquet_path.is_file():
        raise FileNotFoundError(f"Parquet file does not exist: {parquet_path}")
    if if_exists not in {"fail", "append", "replace"}:
        raise ValueError("if_exists must be one of: fail, append, replace")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    table_name = table_name or default_table_name(parquet_path)
    if len(table_name) > 64:
        raise ValueError("MySQL table names cannot exceed 64 characters")
    _quote_identifier(table_name)

    parquet_file = pq.ParquetFile(parquet_path)
    schema = parquet_file.schema_arrow.remove_metadata()
    ddl = create_table_sql(table_name, schema)
    insert_sql = _insert_sql(table_name, schema.names)
    total_rows = parquet_file.metadata.num_rows

    console = Console()
    console.print(
        f"准备导入 [cyan]{parquet_path}[/cyan]\n"
        f"目标：[bold]{DB_CONFIG['database']}.{table_name}[/bold]，"
        f"共 [bold]{total_rows:,}[/bold] 行。"
    )

    factory = connection_factory or _connect_mysql
    connection = factory()
    cursor = connection.cursor()
    inserted_rows = 0
    try:
        exists = _table_exists(cursor, table_name)
        if exists and if_exists == "fail":
            raise FileExistsError(
                f"MySQL table {DB_CONFIG['database']}.{table_name} already exists; "
                "use --if-exists append or --if-exists replace"
            )
        if exists and if_exists == "replace":
            cursor.execute(f"DROP TABLE {_quote_identifier(table_name)}")
            exists = False
        if not exists:
            cursor.execute(ddl)
        elif if_exists == "append":
            _validate_append_columns(cursor, table_name, schema)

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("写入 MySQL", total=total_rows)
            for batch in parquet_file.iter_batches(
                batch_size=batch_size,
                use_threads=True,
            ):
                columns = [column.to_pylist() for column in batch.columns]
                rows: List[Tuple[object, ...]] = [
                    tuple(_normalise_value(value) for value in row)
                    for row in zip(*columns)
                ]
                if rows:
                    cursor.executemany(insert_sql, rows)
                    connection.commit()
                    inserted_rows += len(rows)
                    progress.update(task, advance=len(rows))

        console.print(
            f"[bold green]完成[/bold green]：已向 "
            f"[bold]{DB_CONFIG['database']}.{table_name}[/bold] 写入 "
            f"[bold]{inserted_rows:,}[/bold] 行。"
        )
        return inserted_rows
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream one Parquet file into MySQL with PyMySQL."
    )
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument(
        "--table",
        help="target table name (default: sanitized Parquet filename stem)",
    )
    parser.add_argument(
        "--if-exists",
        choices=["fail", "append", "replace"],
        default="fail",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_import(
            parquet_path=args.parquet,
            table_name=args.table,
            if_exists=args.if_exists,
            batch_size=args.batch_size,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
