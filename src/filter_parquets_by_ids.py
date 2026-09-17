#!/usr/bin/env python3
"""Filter exported item Parquets by (uid, id) pairs from a reference file."""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import FrozenSet, List, Mapping, Optional, Sequence, Tuple

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
DEFAULT_REFERENCE = Path(
    "/DaTa/ljc_codes/ikang-formal/prepare_data/"
    "result_merged_wide_split_50000.parquet"
)
REFERENCE_KEYS: FrozenSet[Tuple[str, str]] = frozenset()


def _as_text(array: pa.Array) -> pa.Array:
    if pa.types.is_string(array.type):
        return array
    if (
        pa.types.is_binary(array.type)
        or pa.types.is_large_binary(array.type)
        or pa.types.is_fixed_size_binary(array.type)
        or pa.types.is_large_string(array.type)
    ):
        return pc.cast(array, pa.string(), safe=True)
    raise TypeError(f"uid/id must be string or binary, got {array.type}")


def load_reference_keys(path: Path) -> Tuple[FrozenSet[Tuple[str, str]], int]:
    schema = pq.ParquetFile(path).schema_arrow
    missing = sorted({"uid", "id"} - set(schema.names))
    if missing:
        raise ValueError(f"{path}: missing required columns: {missing}")

    table = pq.read_table(path, columns=["uid", "id"])
    uid_values = _as_text(table["uid"]).to_pylist()
    id_values = _as_text(table["id"]).to_pylist()
    keys = frozenset(
        (uid, identifier)
        for uid, identifier in zip(uid_values, id_values)
        if uid is not None and identifier is not None
    )
    return keys, table.num_rows


def _init_worker(reference_keys: FrozenSet[Tuple[str, str]]) -> None:
    global REFERENCE_KEYS
    REFERENCE_KEYS = reference_keys


def _filter_one_file(
    source_path_text: str,
    partial_path_text: str,
    compression: Optional[str],
) -> Tuple[str, int, int]:
    source_path = Path(source_path_text)
    partial_path = Path(partial_path_text)
    try:
        parquet_file = pq.ParquetFile(source_path)
        schema = parquet_file.schema_arrow
        missing = sorted({"uid", "id"} - set(schema.names))
        if missing:
            raise ValueError(f"missing required columns: {missing}")

        uid_index = schema.get_field_index("uid")
        id_index = schema.get_field_index("id")
        source_rows = parquet_file.metadata.num_rows
        retained_rows = 0

        with pq.ParquetWriter(
            partial_path,
            schema,
            compression=compression,
            compression_level=3 if compression == "zstd" else None,
        ) as writer:
            for batch in parquet_file.iter_batches(
                batch_size=BATCH_SIZE,
                use_threads=False,
            ):
                uids = _as_text(batch.column(uid_index)).to_pylist()
                identifiers = _as_text(batch.column(id_index)).to_pylist()
                matches = [
                    (uid, identifier) in REFERENCE_KEYS
                    if uid is not None and identifier is not None
                    else False
                    for uid, identifier in zip(uids, identifiers)
                ]
                match_count = sum(matches)
                if match_count:
                    writer.write_batch(batch.filter(pa.array(matches)))
                    retained_rows += match_count
    except Exception as exc:
        raise RuntimeError(f"failed to filter {source_path}: {exc}") from exc

    return source_path.name, source_rows, retained_rows


def _normalise_compression(value: str) -> Optional[str]:
    return None if value == "none" else value


def _default_workers(file_count: int) -> int:
    return max(1, min(8, os.cpu_count() or 1, file_count))


def _make_progress(console: Console) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn("[cyan]{task.fields[rows]:,} retained"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )


def _check_existing_outputs(
    output_dir: Path, source_files: Sequence[Path], overwrite: bool
) -> None:
    targets = [output_dir / path.name for path in source_files]
    targets.append(output_dir / "filter_summary.csv")
    existing = [path for path in targets if path.exists()]
    if existing and not overwrite:
        preview = "\n".join(f"  {path}" for path in existing[:10])
        remainder = len(existing) - 10
        if remainder:
            preview += f"\n  ... and {remainder} more"
        raise FileExistsError(
            "output files already exist; use --overwrite to replace them:\n"
            + preview
        )


def _write_summary(path: Path, results: Sequence[Mapping[str, object]]) -> None:
    ordered = sorted(
        results,
        key=lambda row: (-int(row["retained_rows"]), str(row["filename"])),
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["filename", "source_rows", "retained_rows"],
        )
        writer.writeheader()
        writer.writerows(ordered)


def run_filter(
    source_dir: Path,
    reference_parquet: Path,
    output_dir: Path,
    pattern: str = "*.parquet",
    workers: Optional[int] = None,
    compression: str = "zstd",
    overwrite: bool = False,
) -> List[Mapping[str, object]]:
    source_dir = Path(source_dir).resolve()
    reference_parquet = Path(reference_parquet).resolve()
    output_dir = Path(output_dir).resolve()

    if not source_dir.is_dir():
        raise NotADirectoryError(f"source directory does not exist: {source_dir}")
    if not reference_parquet.is_file():
        raise FileNotFoundError(
            f"reference Parquet does not exist: {reference_parquet}"
        )
    if source_dir == output_dir:
        raise ValueError("source directory and output directory must be different")
    if compression not in {"zstd", "snappy", "none"}:
        raise ValueError("compression must be one of: zstd, snappy, none")

    source_files = sorted(path for path in source_dir.glob(pattern) if path.is_file())
    if not source_files:
        raise FileNotFoundError(
            f"no source files matching {pattern!r} in {source_dir}"
        )

    if workers is None:
        workers = _default_workers(len(source_files))
    if workers < 1:
        raise ValueError("workers must be at least 1")
    workers = min(workers, len(source_files))

    reference_keys, reference_rows = load_reference_keys(reference_parquet)
    if not reference_keys:
        raise ValueError(f"{reference_parquet}: no non-null (uid, id) pairs found")

    output_dir.mkdir(parents=True, exist_ok=True)
    _check_existing_outputs(output_dir, source_files, overwrite)
    partial_root = Path(
        tempfile.mkdtemp(prefix=".id_filter_output_", dir=output_dir)
    )

    console = Console()
    duplicate_count = reference_rows - len(reference_keys)
    console.print(
        f"参考文件包含 [bold]{reference_rows:,}[/bold] 行、"
        f"[bold]{len(reference_keys):,}[/bold] 个唯一非空 (uid, id)；"
        f"待处理 [bold]{len(source_files)}[/bold] 个 Parquet，使用 "
        f"[bold]{workers}[/bold] 个进程。"
    )
    if duplicate_count:
        console.print(
            f"参考文件中有 [yellow]{duplicate_count:,}[/yellow] 行为空键或重复键。"
        )

    results: List[Mapping[str, object]] = []
    try:
        with _make_progress(console) as progress:
            task = progress.add_task(
                "筛选项目文件",
                total=len(source_files),
                rows=0,
            )
            with ProcessPoolExecutor(
                max_workers=workers,
                initializer=_init_worker,
                initargs=(reference_keys,),
            ) as executor:
                futures = [
                    executor.submit(
                        _filter_one_file,
                        str(source_path),
                        str(partial_root / source_path.name),
                        _normalise_compression(compression),
                    )
                    for source_path in source_files
                ]
                retained_total = 0
                for future in as_completed(futures):
                    filename, source_rows, retained_rows = future.result()
                    results.append(
                        {
                            "filename": filename,
                            "source_rows": source_rows,
                            "retained_rows": retained_rows,
                        }
                    )
                    retained_total += retained_rows
                    progress.update(task, advance=1, rows=retained_total)

        summary_partial = partial_root / "filter_summary.csv"
        _write_summary(summary_partial, results)

        for source_path in source_files:
            os.replace(partial_root / source_path.name, output_dir / source_path.name)
        os.replace(summary_partial, output_dir / "filter_summary.csv")

        console.print(
            f"[bold green]完成[/bold green]：共保留 "
            f"[bold]{sum(int(row['retained_rows']) for row in results):,}[/bold] 行。\n"
            f"输出目录：[cyan]{output_dir}[/cyan]\n"
            f"汇总文件：[cyan]{output_dir / 'filter_summary.csv'}[/cyan]"
        )
        return sorted(
            results,
            key=lambda row: (-int(row["retained_rows"]), str(row["filename"])),
        )
    finally:
        shutil.rmtree(partial_root, ignore_errors=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Filter every Parquet in a directory by exact (uid, id) pairs "
            "from a reference Parquet."
        )
    )
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument(
        "--reference-parquet",
        type=Path,
        default=DEFAULT_REFERENCE,
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pattern", default="*.parquet")
    parser.add_argument(
        "--workers",
        type=int,
        help="worker process count (default: min(8, CPU count, source file count))",
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
        run_filter(
            source_dir=args.source_dir,
            reference_parquet=args.reference_parquet,
            output_dir=args.output_dir,
            pattern=args.pattern,
            workers=args.workers,
            compression=args.compression,
            overwrite=args.overwrite,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
