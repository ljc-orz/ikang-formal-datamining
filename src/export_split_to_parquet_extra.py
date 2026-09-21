#!/usr/bin/env python3
"""将划分主表与可配置的额外检验指标表流式导出为一个 Parquet。"""

from __future__ import annotations

import argparse
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Sequence

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
    "database": "datas",
    "charset": "utf8mb4",
    "unix_socket": "/DaTa/mysql/mysql.sock",
    "local_infile": True,
    "autocommit": False,
}

DEFAULT_BASE_DATABASE = "datas"
DEFAULT_BASE_TABLE = "result_merged_wide_split_50000"
DEFAULT_EXTRA_DATABASE = "datas_in_progress"
DEFAULT_OUTPUT = "result_merged_wide_split_50000_extra.parquet"
DEFAULT_BATCH_SIZE = 5_000
IDENTIFIER_PATTERN = re.compile(r"[A-Za-z0-9_]+")

BASE_FIELDS = [
    "uid",
    "id",
    "hospid",
    "usex",
    "examage",
    "image_path_1",
    "image_path_2",
]

BASE_SCHEMA_FIELDS = [
    pa.field("uid", pa.string()),
    pa.field("id", pa.string()),
    pa.field("hospid", pa.int64()),
    pa.field("usex", pa.string()),
    pa.field("examage", pa.float64()),
    pa.field("image_path_1", pa.string()),
    pa.field("image_path_2", pa.string()),
]


@dataclass(frozen=True)
class ItemMapping:
    table: str
    output_field: str


def _validate_identifier(value: str, description: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"非法{description}：{value!r}")
    return value


def parse_item_mapping(value: str) -> ItemMapping:
    if value.count("=") != 1:
        raise argparse.ArgumentTypeError(
            f"指标映射必须使用 表名=输出字段名 格式，收到：{value!r}"
        )
    table, output_field = (part.strip() for part in value.split("=", 1))
    try:
        _validate_identifier(table, "指标表名")
        _validate_identifier(output_field, "输出字段名")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return ItemMapping(table=table, output_field=output_field)


def build_parquet_schema(items: Sequence[ItemMapping]) -> pa.Schema:
    return pa.schema(
        BASE_SCHEMA_FIELDS
        + [pa.field(item.output_field, pa.int8()) for item in items]
        + [pa.field("split", pa.string())]
    )


def build_select_sql(
    base_database: str,
    base_table: str,
    extra_database: str,
    items: Sequence[ItemMapping],
) -> str:
    base_columns = [f"base.`{field}`" for field in BASE_FIELDS]
    extra_columns = []
    for index, item in enumerate(items):
        alias = f"extra_{index}"
        extra_columns.append(
            "CASE "
            f"WHEN {alias}.`itemresult` IS NULL "
            f"OR {alias}.`normallowvalue` IS NULL "
            f"OR {alias}.`normalhighvalue` IS NULL THEN NULL "
            f"WHEN {alias}.`itemresult` BETWEEN "
            f"{alias}.`normallowvalue` AND {alias}.`normalhighvalue` THEN 0 "
            f"ELSE 1 END AS `{item.output_field}`"
        )
    columns = base_columns + extra_columns + ["base.`split`"]

    joins = []
    for index, item in enumerate(items):
        alias = f"extra_{index}"
        joins.append(
            f"INNER JOIN `{extra_database}`.`{item.table}` AS {alias} "
            f"ON {alias}.`uid` = base.`uid` AND {alias}.`id` = base.`id`"
        )

    return (
        "SELECT "
        + ", ".join(columns)
        + f" FROM `{base_database}`.`{base_table}` AS base "
        + " ".join(joins)
        + " ORDER BY base.`uid`, base.`id`"
    )


def _connect_mysql():
    try:
        import pymysql
        from pymysql.cursors import SSDictCursor
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyMySQL 未安装。请执行："
            "conda run -n ikang python -m pip install PyMySQL"
        ) from exc
    return pymysql.connect(cursorclass=SSDictCursor, **DB_CONFIG)


def iter_mysql_batches(
    connection,
    sql: str,
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    """使用服务端游标逐批读取，避免一次性加载整个查询结果。"""
    with connection.cursor() as cursor:
        cursor.execute(sql)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield list(rows)


def export_parquet(
    base_database: str,
    base_table: str,
    extra_database: str,
    items: Sequence[ItemMapping],
    output: Path,
    batch_size: int,
    expected_rows: int,
    connection_factory: Optional[Callable[[], object]] = None,
) -> int:
    output = Path(output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    schema = build_parquet_schema(items)
    sql = build_select_sql(base_database, base_table, extra_database, items)

    temp_handle = tempfile.NamedTemporaryFile(
        prefix=f".{output.name}.",
        suffix=".tmp",
        dir=output.parent,
        delete=False,
    )
    temp_path = Path(temp_handle.name)
    temp_handle.close()

    connection = None
    writer: Optional[pq.ParquetWriter] = None
    exported_rows = 0
    console = Console()
    try:
        connection = (connection_factory or _connect_mysql)()
        writer = pq.ParquetWriter(
            temp_path,
            schema,
            compression="zstd",
            use_dictionary=["usex", "split"],
            write_statistics=True,
        )
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
            task = progress.add_task(
                "导出 MySQL",
                total=expected_rows if expected_rows else None,
            )
            for rows in iter_mysql_batches(connection, sql, batch_size):
                arrow_table = pa.Table.from_pylist(rows, schema=schema)
                writer.write_table(arrow_table, row_group_size=batch_size)
                exported_rows += len(rows)
                progress.update(task, advance=len(rows))

        writer.close()
        writer = None

        if expected_rows and exported_rows != expected_rows:
            raise RuntimeError(
                f"导出行数为 {exported_rows}，不等于期望的 {expected_rows}；"
                "临时文件不会作为最终结果保留"
            )

        metadata = pq.read_metadata(temp_path)
        if metadata.num_rows != exported_rows:
            raise RuntimeError(
                f"Parquet 元数据行数 {metadata.num_rows} 与读取行数 "
                f"{exported_rows} 不一致"
            )

        os.replace(temp_path, output)
        return exported_rows
    except Exception:
        if writer is not None:
            writer.close()
        temp_path.unlink(missing_ok=True)
        raise
    finally:
        if connection is not None:
            connection.close()


def print_verification(output: Path) -> None:
    parquet_file = pq.ParquetFile(output)
    console = Console()
    console.print(f"文件：[cyan]{output.resolve()}[/cyan]")
    console.print(f"行数：[bold]{parquet_file.metadata.num_rows:,}[/bold]")
    console.print(f"Row groups：[bold]{parquet_file.metadata.num_row_groups}[/bold]")
    if parquet_file.metadata.num_rows:
        first_record = parquet_file.read_row_group(0).slice(0, 1).to_pylist()[0]
        console.print("第一条记录：")
        console.print(first_record)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-database",
        default=DEFAULT_BASE_DATABASE,
        help=f"划分主表所在数据库（默认：{DEFAULT_BASE_DATABASE}）",
    )
    parser.add_argument(
        "--base-table",
        default=DEFAULT_BASE_TABLE,
        help=f"划分主表名（默认：{DEFAULT_BASE_TABLE}）",
    )
    parser.add_argument(
        "--extra-database",
        default=DEFAULT_EXTRA_DATABASE,
        help=f"额外指标表所在数据库（默认：{DEFAULT_EXTRA_DATABASE}）",
    )
    parser.add_argument(
        "--item",
        action="append",
        type=parse_item_mapping,
        required=True,
        metavar="TABLE=OUTPUT_FIELD",
        help=(
            "额外指标表与输出列映射；根据表中的 itemresult、"
            "normallowvalue、normalhighvalue 生成 0/1/NULL。可重复传入多次"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(DEFAULT_OUTPUT),
        help=f"输出 Parquet 路径（默认：{DEFAULT_OUTPUT}）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"每批读取行数（默认：{DEFAULT_BATCH_SIZE}）",
    )
    parser.add_argument(
        "--expected-rows",
        type=int,
        default=50_000,
        help="期望导出的行数；设为 0 则不校验（默认：50000）",
    )
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有输出文件")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    _validate_identifier(args.base_database, "主数据库名")
    _validate_identifier(args.base_table, "主表名")
    _validate_identifier(args.extra_database, "指标数据库名")
    if args.batch_size <= 0:
        raise ValueError("--batch-size 必须大于 0")
    if args.expected_rows < 0:
        raise ValueError("--expected-rows 不能小于 0")

    output_fields = [item.output_field for item in args.item]
    duplicate_fields = sorted(
        field for field in set(output_fields) if output_fields.count(field) > 1
    )
    conflicts = sorted(set(output_fields) & (set(BASE_FIELDS) | {"split"}))
    if duplicate_fields:
        raise ValueError(f"重复的输出字段名：{duplicate_fields}")
    if conflicts:
        raise ValueError(f"输出字段名与基础字段冲突：{conflicts}")
    if args.output.exists() and not args.overwrite:
        raise FileExistsError(
            f"输出文件已存在：{args.output}。如需覆盖，请增加 --overwrite。"
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate_args(args)
        exported_rows = export_parquet(
            base_database=args.base_database,
            base_table=args.base_table,
            extra_database=args.extra_database,
            items=args.item,
            output=args.output,
            batch_size=args.batch_size,
            expected_rows=args.expected_rows,
        )
        Console().print(f"[bold green]导出完成[/bold green]，共 {exported_rows:,} 行。")
        print_verification(args.output.expanduser().resolve())
    except Exception as exc:
        Console(stderr=True).print(f"[bold red]ERROR[/bold red]：{exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
