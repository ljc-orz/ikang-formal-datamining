import csv
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from export_items import run_export


SOURCE_SCHEMA = pa.schema(
    [
        pa.field("uid", pa.binary()),
        pa.field("id", pa.binary()),
        pa.field("hospid", pa.binary()),
        pa.field("hospname", pa.binary()),
        pa.field("regdate", pa.binary()),
        pa.field("examage", pa.float64()),
        pa.field("usex", pa.binary()),
        pa.field("deptid", pa.int64()),
        pa.field("dept", pa.binary()),
        pa.field("checkitemcode", pa.binary()),
        pa.field("checkitemname", pa.binary()),
        pa.field("itemcode", pa.binary()),
        pa.field("itemname", pa.binary()),
        pa.field("itemresult", pa.binary()),
        pa.field("normalhighvalue", pa.binary()),
        pa.field("normallowvalue", pa.binary()),
        pa.field("itemindexresultunit", pa.binary()),
        pa.field("rn", pa.int64()),
    ]
)


def make_row(identifier, code, name, result, high, low, rn):
    return {
        "uid": f"uid-{identifier}".encode(),
        "id": identifier.encode(),
        "hospid": b"944",
        "hospname": "爱康体检中心".encode(),
        "regdate": b"2017-08-16",
        "examage": 36.0,
        "usex": "男".encode(),
        "deptid": 2,
        "dept": "检验科".encode(),
        "checkitemcode": b"check-1",
        "checkitemname": "检查组合".encode(),
        "itemcode": code.encode(),
        "itemname": name.encode(),
        "itemresult": None if result is None else result.encode(),
        "normalhighvalue": None if high is None else high.encode(),
        "normallowvalue": None if low is None else low.encode(),
        "itemindexresultunit": b"mmol/L",
        "rn": rn,
    }


def write_source(path, rows):
    columns = {
        field.name: pa.array([row[field.name] for row in rows], type=field.type)
        for field in SOURCE_SCHEMA
    }
    pq.write_table(pa.table(columns, schema=SOURCE_SCHEMA), path)


class ExportItemsTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.input_dir = self.root / "input"
        self.input_dir.mkdir()
        self.items_csv = self.root / "items.csv"

        with self.items_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["itemname", "itemcode"])
            writer.writerow(["目标/项目", "2.1"])
            writer.writerow(["无记录项目", "4.1"])
            writer.writerow(["第三项", "3.1"])

        write_source(
            self.input_dir / "a.parq",
            [
                make_row("a1", "2.1", "源名称甲", "0.302", None, "0.1", 1),
                make_row("ignored", "9.9", "非目标", "1", "2", "0", 2),
            ],
        )
        write_source(
            self.input_dir / "b.parq",
            [
                make_row("b1", "2.1", "源名称乙", "阳性", "无法判断", None, 1),
                make_row("b2", "3.1", "第三项", None, "5", "1", 2),
            ],
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _run(self, output_name, workers, overwrite=False):
        output_dir = self.root / output_name
        counts = run_export(
            input_dir=self.input_dir,
            items_csv=self.items_csv,
            output_dir=output_dir,
            workers=workers,
            overwrite=overwrite,
        )
        return output_dir, counts

    def test_export_decodes_filters_and_writes_empty_item(self):
        output_dir, counts = self._run("parallel", workers=2)

        self.assertEqual(counts, {"2.1": 2, "4.1": 0, "3.1": 1})
        target_path = output_dir / "目标_项目__2.1.parquet"
        empty_path = output_dir / "无记录项目__4.1.parquet"
        third_path = output_dir / "第三项__3.1.parquet"
        self.assertTrue(target_path.is_file())
        self.assertTrue(empty_path.is_file())
        self.assertTrue(third_path.is_file())

        target = pq.read_table(target_path)
        self.assertNotIn("rn", target.column_names)
        self.assertEqual(target["id"].to_pylist(), ["a1", "b1"])
        self.assertEqual(target["itemname"].to_pylist(), ["源名称甲", "源名称乙"])
        self.assertEqual(target["itemresult"].to_pylist(), ["0.302", "阳性"])
        self.assertEqual(target["normalhighvalue"].to_pylist(), [None, "无法判断"])
        self.assertEqual(target["normallowvalue"].to_pylist(), ["0.1", None])
        self.assertEqual(target.schema.field("uid").type, pa.string())
        self.assertEqual(target.schema.field("itemresult").type, pa.string())
        self.assertEqual(target.schema.field("examage").type, pa.float64())
        self.assertEqual(target.schema.field("deptid").type, pa.int64())

        empty = pq.read_table(empty_path)
        self.assertEqual(empty.num_rows, 0)
        self.assertEqual(empty.schema, target.schema)

        with (output_dir / "export_summary.csv").open(
            "r", encoding="utf-8", newline=""
        ) as handle:
            summary = list(csv.DictReader(handle))
        self.assertEqual([row["rows"] for row in summary], ["2", "0", "1"])
        self.assertEqual(summary[0]["filename"], "目标_项目__2.1.parquet")

    def test_single_and_multiple_workers_produce_same_rows(self):
        single_dir, _ = self._run("single", workers=1)
        parallel_dir, _ = self._run("multiple", workers=2)

        for filename in [
            "目标_项目__2.1.parquet",
            "无记录项目__4.1.parquet",
            "第三项__3.1.parquet",
        ]:
            single = pq.read_table(single_dir / filename)
            parallel = pq.read_table(parallel_dir / filename)
            self.assertEqual(single.schema, parallel.schema)
            self.assertEqual(single.to_pydict(), parallel.to_pydict())

    def test_existing_outputs_require_explicit_overwrite(self):
        output_dir, _ = self._run("overwrite", workers=1)
        with self.assertRaises(FileExistsError):
            run_export(
                input_dir=self.input_dir,
                items_csv=self.items_csv,
                output_dir=output_dir,
                workers=1,
            )

        counts = run_export(
            input_dir=self.input_dir,
            items_csv=self.items_csv,
            output_dir=output_dir,
            workers=1,
            overwrite=True,
        )
        self.assertEqual(counts["2.1"], 2)

    def test_schema_mismatch_names_the_source_file(self):
        mismatched_dir = self.root / "mismatched"
        mismatched_dir.mkdir()
        write_source(
            mismatched_dir / "a.parq",
            [make_row("a1", "2.1", "源名称甲", "1", "2", "0", 1)],
        )

        mismatched_schema = SOURCE_SCHEMA.set(
            SOURCE_SCHEMA.get_field_index("deptid"),
            pa.field("deptid", pa.int32()),
        )
        row = make_row("b1", "2.1", "源名称乙", "1", "2", "0", 1)
        columns = {
            field.name: pa.array([row[field.name]], type=field.type)
            for field in mismatched_schema
        }
        pq.write_table(
            pa.table(columns, schema=mismatched_schema),
            mismatched_dir / "b.parq",
        )

        with self.assertRaisesRegex(RuntimeError, r"b\.parq.*schema differs"):
            run_export(
                input_dir=mismatched_dir,
                items_csv=self.items_csv,
                output_dir=self.root / "mismatch-output",
                workers=2,
            )


if __name__ == "__main__":
    unittest.main()
