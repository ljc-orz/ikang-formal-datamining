import csv
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from filter_parquets_by_ids import run_filter


class FilterParquetsByIdsTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.source_dir = self.root / "source"
        self.source_dir.mkdir()
        self.reference = self.root / "reference.parquet"

        pq.write_table(
            pa.table(
                {
                    "uid": pa.array([b"u1", b"u2", b"u1"], type=pa.binary()),
                    "id": pa.array([b"i1", b"i2", b"i1"], type=pa.binary()),
                    "unused": [1, 2, 3],
                }
            ),
            self.reference,
        )

        schema = pa.schema(
            [
                pa.field("uid", pa.string()),
                pa.field("id", pa.string()),
                pa.field("value", pa.string()),
            ]
        )
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {"uid": "u1", "id": "i1", "value": "keep-1"},
                    {"uid": "u1", "id": "i2", "value": "wrong-pair"},
                    {"uid": "u2", "id": "i2", "value": "keep-2"},
                ],
                schema=schema,
            ),
            self.source_dir / "a.parquet",
        )
        pq.write_table(
            pa.Table.from_pylist(
                [{"uid": "u3", "id": "i3", "value": "drop"}],
                schema=schema,
            ),
            self.source_dir / "b.parquet",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_filters_exact_pairs_and_sorts_summary(self):
        output_dir = self.root / "output"
        results = run_filter(
            source_dir=self.source_dir,
            reference_parquet=self.reference,
            output_dir=output_dir,
            workers=2,
        )

        self.assertEqual(
            [(row["filename"], row["retained_rows"]) for row in results],
            [("a.parquet", 2), ("b.parquet", 0)],
        )
        filtered_a = pq.read_table(output_dir / "a.parquet")
        self.assertEqual(filtered_a["value"].to_pylist(), ["keep-1", "keep-2"])
        self.assertEqual(
            filtered_a.schema,
            pq.read_table(self.source_dir / "a.parquet").schema,
        )
        self.assertEqual(pq.read_table(output_dir / "b.parquet").num_rows, 0)

        with (output_dir / "filter_summary.csv").open(
            "r", encoding="utf-8", newline=""
        ) as handle:
            summary = list(csv.DictReader(handle))
        self.assertEqual([row["filename"] for row in summary], ["a.parquet", "b.parquet"])
        self.assertEqual([row["retained_rows"] for row in summary], ["2", "0"])

    def test_existing_output_requires_overwrite(self):
        output_dir = self.root / "output-overwrite"
        run_filter(
            source_dir=self.source_dir,
            reference_parquet=self.reference,
            output_dir=output_dir,
            workers=1,
        )
        with self.assertRaises(FileExistsError):
            run_filter(
                source_dir=self.source_dir,
                reference_parquet=self.reference,
                output_dir=output_dir,
                workers=1,
            )
        run_filter(
            source_dir=self.source_dir,
            reference_parquet=self.reference,
            output_dir=output_dir,
            workers=1,
            overwrite=True,
        )


if __name__ == "__main__":
    unittest.main()
