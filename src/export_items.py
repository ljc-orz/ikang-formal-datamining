#!/usr/bin/env python3
"""Export selected itemcodes from a directory of Parquet files."""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.compute as pc
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


BATCH_SIZE = 131_072
ROW_GROUP_SIZE = 131_072
FORCED_STRING_COLUMNS = {
    "itemresult",
    "normalhighvalue",
    "normallowvalue",
}
REQUIRED_COLUMNS = FORCED_STRING_COLUMNS | {"itemcode", "rn"}


@dataclass(frozen=True)
class Item:
    itemname: str
    itemcode: str
    filename: str


def _safe_filename_part(value: str) -> str:
    """Replace characters that would create paths instead of filenames."""
    return value.replace("/", "_").replace("\\", "_").replace("\0", "_")


def _item_filename(itemname: str, itemcode: str) -> str:
    name = _safe_filename_part(itemname)
    code = _safe_filename_part(itemcode)
    return f"{name}__{code}.parquet"


def read_items(path: Path) -> List[Item]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or not {"itemname", "itemcode"}.issubset(
            reader.fieldnames
        ):
            raise ValueError(
                f"{path}: CSV must contain itemname and itemcode columns"
            )

        items: List[Item] = []
        seen_codes = set()
        seen_filenames = set()
        for line_number, row in enumerate(reader, start=2):
            itemname = (row.get("itemname") or "").strip()
            itemcode = (row.get("itemcode") or "").strip()
            if not itemname or not itemcode:
                raise ValueError(
                    f"{path}:{line_number}: itemname and itemcode cannot be empty"
                )
            if itemcode in seen_codes:
                raise ValueError(
                    f"{path}:{line_number}: duplicate itemcode {itemcode!r}"
                )

            filename = _item_filename(itemname, itemcode)
            if filename in seen_filenames:
                raise ValueError(
                    f"{path}:{line_number}: output filename collision: {filename}"
                )
            seen_codes.add(itemcode)
            seen_filenames.add(filename)
            items.append(Item(itemname, itemcode, filename))

    if not items:
        raise ValueError(f"{path}: no items found")
    return items


def _is_binary(data_type: pa.DataType) -> bool:
    return (
        pa.types.is_binary(data_type)
        or pa.types.is_large_binary(data_type)
        or pa.types.is_fixed_size_binary(data_type)
    )


def _output_schema(source_schema: pa.Schema) -> pa.Schema:
    missing = sorted(REQUIRED_COLUMNS - set(source_schema.names))
    if missing:
        raise ValueError(f"source schema is missing required columns: {missing}")

    itemcode_type = source_schema.field("itemcode").type
    if not (_is_binary(itemcode_type) or pa.types.is_string(itemcode_type)):
        raise ValueError(
            "itemcode must have a binary or string Arrow type, "
            f"got {itemcode_type}"
        )

    fields = []
    for field in source_schema:
        if field.name == "rn":
            continue
        output_type = field.type
        if field.name in FORCED_STRING_COLUMNS or _is_binary(field.type):
            output_type = pa.string()
        fields.append(
            pa.field(
                field.name,
                output_type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
        )
    # Pandas metadata refers to the input schema (including rn), so it must not
    # be copied to the changed output schema.
    return pa.schema(fields)


def _as_utf8(array: pa.Array) -> pa.Array:
    if pa.types.is_string(array.type):
        return array
    return pc.cast(array, pa.string(), safe=True)


def _transform_selected_batch(
    batch: pa.RecordBatch, output_schema: pa.Schema
) -> pa.RecordBatch:
    arrays = []
    for field in output_schema:
        array = batch.column(batch.schema.get_field_index(field.name))
        if field.type == pa.string() and array.type != pa.string():
            array = pc.cast(array, pa.string(), safe=True)
        arrays.append(array)
    return pa.RecordBatch.from_arrays(arrays, schema=output_schema)


def _process_source_file(
    source_path_text: str,
    source_index: int,
    expected_schema: pa.Schema,
    output_schema: pa.Schema,
    itemcodes: Sequence[str],
    fragment_dirs: Sequence[str],
) -> Tuple[int, Dict[str, int]]:
    """Read one source file once and write one fragment per matching item."""
    source_path = Path(source_path_text)
    writers: Dict[str, pq.ParquetWriter] = {}
    counts = {code: 0 for code in itemcodes}
    code_to_dir = dict(zip(itemcodes, fragment_dirs))

    try:
        parquet_file = pq.ParquetFile(source_path)
        actual_schema = parquet_file.schema_arrow.remove_metadata()
        if not actual_schema.equals(expected_schema, check_metadata=False):
            raise ValueError(
                "schema differs from the first source file\n"
                f"expected: {expected_schema}\nactual: {actual_schema}"
            )

        itemcode_index = actual_schema.get_field_index("itemcode")
        target_values = pa.array(itemcodes, type=pa.string())

        for batch in parquet_file.iter_batches(
            batch_size=BATCH_SIZE,
            columns=actual_schema.names,
            use_threads=False,
        ):
            decoded_codes = _as_utf8(batch.column(itemcode_index))
            target_mask = pc.is_in(decoded_codes, value_set=target_values)
            if not pc.any(target_mask).as_py():
                continue

            selected = batch.filter(target_mask)
            transformed = _transform_selected_batch(selected, output_schema)
            transformed_codes = transformed.column(
                transformed.schema.get_field_index("itemcode")
            )
            present_codes = pc.unique(transformed_codes).to_pylist()

            for code in present_codes:
                if code is None or code not in code_to_dir:
                    continue
                item_mask = pc.equal(transformed_codes, pa.scalar(code))
                item_batch = transformed.filter(item_mask)
                if item_batch.num_rows == 0:
                    continue

                writer = writers.get(code)
                if writer is None:
                    fragment_path = (
                        Path(code_to_dir[code]) / f"{source_index:08d}.parquet"
                    )
                    writer = pq.ParquetWriter(
                        fragment_path,
                        output_schema,
                        compression="snappy",
                    )
                    writers[code] = writer
                writer.write_batch(item_batch)
                counts[code] += item_batch.num_rows
    except Exception as exc:
        raise RuntimeError(f"failed to process source file {source_path}: {exc}") from exc
    finally:
        for writer in writers.values():
            writer.close()

    return source_index, counts


def _write_pending_rows(
    writer: pq.ParquetWriter,
    pending: List[pa.RecordBatch],
    schema: pa.Schema,
    flush_all: bool,
) -> Tuple[List[pa.RecordBatch], int]:
    if not pending:
        return [], 0

    table = pa.Table.from_batches(pending, schema=schema)
    rows_to_write = table.num_rows if flush_all else (
        table.num_rows // ROW_GROUP_SIZE
    ) * ROW_GROUP_SIZE
    offset = 0
    while offset < rows_to_write:
        length = min(ROW_GROUP_SIZE, rows_to_write - offset)
        writer.write_table(table.slice(offset, length), row_group_size=ROW_GROUP_SIZE)
        offset += length

    remainder = table.slice(offset)
    return remainder.to_batches() if remainder.num_rows else [], rows_to_write


def _merge_item_fragments(
    item_index: int,
    item: Item,
    fragment_dir_text: str,
    partial_dir_text: str,
    output_schema: pa.Schema,
    compression: Optional[str],
) -> Tuple[int, int, str]:
    fragment_dir = Path(fragment_dir_text)
    partial_path = Path(partial_dir_text) / item.filename
    fragments = sorted(fragment_dir.glob("*.parquet"))
    pending: List[pa.RecordBatch] = []
    pending_rows = 0
    total_rows = 0

    try:
        with pq.ParquetWriter(
            partial_path,
            output_schema,
            compression=compression,
            compression_level=3 if compression == "zstd" else None,
        ) as writer:
            for fragment in fragments:
                parquet_file = pq.ParquetFile(fragment)
                for batch in parquet_file.iter_batches(batch_size=ROW_GROUP_SIZE):
                    pending.append(batch)
                    pending_rows += batch.num_rows
                    total_rows += batch.num_rows
                    if pending_rows >= ROW_GROUP_SIZE:
                        pending, written = _write_pending_rows(
                            writer, pending, output_schema, flush_all=False
                        )
                        pending_rows -= written

            if pending:
                pending, written = _write_pending_rows(
                    writer, pending, output_schema, flush_all=True
                )
                pending_rows -= written
    except Exception as exc:
        raise RuntimeError(f"failed to merge item {item.itemcode}: {exc}") from exc

    if pending or pending_rows != 0:
        raise RuntimeError(f"internal merge error for item {item.itemcode}")
    return item_index, total_rows, str(partial_path)


def _normalise_compression(value: str) -> Optional[str]:
    return None if value == "none" else value


def _default_workers(source_count: int) -> int:
    return max(1, min(8, os.cpu_count() or 1, source_count))


def _make_progress(console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn("[cyan]{task.fields[rows]:,} rows"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )


def _check_existing_outputs(
    output_dir: Path, items: Sequence[Item], overwrite: bool
) -> None:
    targets = [output_dir / item.filename for item in items]
    targets.append(output_dir / "export_summary.csv")
    existing = [path for path in targets if path.exists()]
    if existing and not overwrite:
        preview = "\n".join(f"  {path}" for path in existing[:10])
        remainder = len(existing) - 10
        if remainder > 0:
            preview += f"\n  ... and {remainder} more"
        raise FileExistsError(
            "output files already exist; use --overwrite to replace them:\n"
            + preview
        )


def _write_summary(path: Path, items: Sequence[Item], counts: Sequence[int]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["itemname", "itemcode", "rows", "filename"],
        )
        writer.writeheader()
        for item, row_count in zip(items, counts):
            writer.writerow(
                {
                    "itemname": item.itemname,
                    "itemcode": item.itemcode,
                    "rows": row_count,
                    "filename": item.filename,
                }
            )


def run_export(
    input_dir: Path,
    items_csv: Path,
    output_dir: Path,
    pattern: str = "*.parq",
    workers: Optional[int] = None,
    staging_dir: Optional[Path] = None,
    compression: str = "zstd",
    overwrite: bool = False,
) -> Mapping[str, int]:
    input_dir = Path(input_dir).resolve()
    items_csv = Path(items_csv).resolve()
    output_dir = Path(output_dir).resolve()
    staging_parent = Path(staging_dir).resolve() if staging_dir else output_dir

    if not input_dir.is_dir():
        raise NotADirectoryError(f"input directory does not exist: {input_dir}")
    if not items_csv.is_file():
        raise FileNotFoundError(f"items CSV does not exist: {items_csv}")
    if compression not in {"zstd", "snappy", "none"}:
        raise ValueError("compression must be one of: zstd, snappy, none")

    items = read_items(items_csv)
    source_files = sorted(path for path in input_dir.glob(pattern) if path.is_file())
    if not source_files:
        raise FileNotFoundError(
            f"no source files matching {pattern!r} in {input_dir}"
        )

    first_schema = pq.ParquetFile(source_files[0]).schema_arrow.remove_metadata()
    output_schema = _output_schema(first_schema)

    if workers is None:
        workers = _default_workers(len(source_files))
    if workers < 1:
        raise ValueError("workers must be at least 1")
    workers = min(workers, len(source_files))

    output_dir.mkdir(parents=True, exist_ok=True)
    staging_parent.mkdir(parents=True, exist_ok=True)
    _check_existing_outputs(output_dir, items, overwrite)

    stage_root = Path(
        tempfile.mkdtemp(prefix="item_export_stage_", dir=staging_parent)
    )
    partial_root = Path(
        tempfile.mkdtemp(prefix=".item_export_output_", dir=output_dir)
    )

    itemcodes = [item.itemcode for item in items]
    fragment_dirs = []
    for index in range(len(items)):
        path = stage_root / "fragments" / f"{index:04d}"
        path.mkdir(parents=True)
        fragment_dirs.append(path)

    console = Console()
    console.print(
        f"发现 [bold]{len(source_files)}[/bold] 个源文件、"
        f"[bold]{len(items)}[/bold] 个目标项目，使用 "
        f"[bold]{workers}[/bold] 个进程。"
    )

    extracted_counts = {code: 0 for code in itemcodes}
    try:
        with _make_progress(console) as progress:
            extract_task = progress.add_task(
                "提取源文件",
                total=len(source_files),
                rows=0,
            )
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(
                        _process_source_file,
                        str(source_path),
                        source_index,
                        first_schema,
                        output_schema,
                        itemcodes,
                        [str(path) for path in fragment_dirs],
                    )
                    for source_index, source_path in enumerate(source_files)
                ]
                selected_rows = 0
                for future in as_completed(futures):
                    _, counts = future.result()
                    for code, count in counts.items():
                        extracted_counts[code] += count
                    selected_rows += sum(counts.values())
                    progress.update(
                        extract_task,
                        advance=1,
                        rows=selected_rows,
                    )

        merge_workers = min(workers, len(items))
        merged_results: List[Optional[Tuple[int, int, str]]] = [None] * len(items)
        with _make_progress(console) as progress:
            merge_task = progress.add_task(
                "合并项目文件",
                total=len(items),
                rows=0,
            )
            with ProcessPoolExecutor(max_workers=merge_workers) as executor:
                futures = [
                    executor.submit(
                        _merge_item_fragments,
                        item_index,
                        item,
                        str(fragment_dirs[item_index]),
                        str(partial_root),
                        output_schema,
                        _normalise_compression(compression),
                    )
                    for item_index, item in enumerate(items)
                ]
                merged_rows = 0
                for future in as_completed(futures):
                    item_index, row_count, partial_path = future.result()
                    merged_results[item_index] = (item_index, row_count, partial_path)
                    merged_rows += row_count
                    progress.update(
                        merge_task,
                        advance=1,
                        rows=merged_rows,
                    )

        final_counts = []
        for index, result in enumerate(merged_results):
            if result is None:
                raise RuntimeError(f"missing merge result for {items[index].itemcode}")
            _, row_count, _ = result
            expected_count = extracted_counts[items[index].itemcode]
            if row_count != expected_count:
                raise RuntimeError(
                    f"row count mismatch for {items[index].itemcode}: "
                    f"extracted {expected_count}, merged {row_count}"
                )
            final_counts.append(row_count)

        summary_partial = partial_root / "export_summary.csv"
        _write_summary(summary_partial, items, final_counts)

        for item, result in zip(items, merged_results):
            assert result is not None
            os.replace(result[2], output_dir / item.filename)
        os.replace(summary_partial, output_dir / "export_summary.csv")

        total_rows = sum(final_counts)
        console.print(
            f"[bold green]完成[/bold green]：生成 [bold]{len(items)}[/bold] 个 "
            f"Parquet 文件，共 [bold]{total_rows:,}[/bold] 行。\n"
            f"输出目录：[cyan]{output_dir}[/cyan]"
        )
        return dict(zip(itemcodes, final_counts))
    finally:
        shutil.rmtree(stage_root, ignore_errors=True)
        shutil.rmtree(partial_root, ignore_errors=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export one Parquet file per itemcode listed in items.csv. "
            "Source files are scanned once in parallel."
        )
    )
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument(
        "--items-csv",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "items.csv",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pattern", default="*.parq")
    parser.add_argument(
        "--workers",
        type=int,
        help="worker process count (default: min(8, CPU count, source file count))",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        help="parent directory for temporary fragments (default: output directory)",
    )
    parser.add_argument(
        "--compression",
        choices=["zstd", "snappy", "none"],
        default="zstd",
    )
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_export(
            input_dir=args.input_dir,
            items_csv=args.items_csv,
            output_dir=args.output_dir,
            pattern=args.pattern,
            workers=args.workers,
            staging_dir=args.staging_dir,
            compression=args.compression,
            overwrite=args.overwrite,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
