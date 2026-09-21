import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from export_split_to_parquet_extra import (
    BASE_FIELDS,
    ItemMapping,
    build_parquet_schema,
    build_select_sql,
    export_parquet,
    parse_item_mapping,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.offset = 0
        self.sql = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, sql):
        self.sql = sql

    def fetchmany(self, size):
        result = self.rows[self.offset : self.offset + size]
        self.offset += len(result)
        return result


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.cursor_instance = FakeCursor(rows)
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def make_row(uid, result_ldl_c, result_ua, split):
    row = {
        "uid": uid,
        "id": f"id-{uid}",
        "hospid": 944,
        "usex": "MAN",
        "examage": 36.0,
        "image_path_1": f"/{uid}-1.jpg",
        "image_path_2": f"/{uid}-2.jpg",
        "split": split,
    }
    row["result_ldl_c"] = result_ldl_c
    row["result_ua"] = result_ua
    return row


class ExportSplitToParquetExtraTest(unittest.TestCase):
    def test_mapping_and_dynamic_schema(self):
        items = [
            parse_item_mapping("lab_item_ldl_c=result_ldl_c"),
            parse_item_mapping("lab_item_ua=result_ua"),
        ]
        self.assertEqual(items[0], ItemMapping("lab_item_ldl_c", "result_ldl_c"))
        schema = build_parquet_schema(items)
        self.assertEqual(
            schema.names,
            BASE_FIELDS + ["result_ldl_c", "result_ua", "split"],
        )
        self.assertEqual(schema.names[-3:], ["result_ldl_c", "result_ua", "split"])
        self.assertEqual(schema.field("result_ldl_c").type, pa.int8())
        self.assertNotIn("result_alt", schema.names)

        sql = build_select_sql("datas", "base_table", "extras", items)
        self.assertIn("`extras`.`lab_item_ldl_c`", sql)
        self.assertIn("extra_0.`itemresult` IS NULL", sql)
        self.assertIn("extra_0.`normallowvalue` IS NULL", sql)
        self.assertIn("extra_0.`normalhighvalue` IS NULL", sql)
        self.assertIn(
            "extra_0.`itemresult` BETWEEN extra_0.`normallowvalue` "
            "AND extra_0.`normalhighvalue` THEN 0 ELSE 1 END AS `result_ldl_c`",
            sql,
        )
        self.assertIn("extra_1.`uid` = base.`uid`", sql)
        self.assertIn("base.`split`", sql)
        self.assertNotIn("base.`result_alt`", sql)
        self.assertTrue(sql.endswith("ORDER BY base.`uid`, base.`id`"))

    def test_streaming_export(self):
        items = [
            ItemMapping("lab_item_ldl_c", "result_ldl_c"),
            ItemMapping("lab_item_ua", "result_ua"),
        ]
        rows = [
            make_row("u1", 0, 1, "train"),
            make_row("u2", 1, None, "internal_validation"),
        ]
        connection = FakeConnection(rows)

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "extra.parquet"
            exported = export_parquet(
                base_database="datas",
                base_table="base_table",
                extra_database="extras",
                items=items,
                output=output,
                batch_size=1,
                expected_rows=2,
                connection_factory=lambda: connection,
            )
            table = pq.read_table(output)

        self.assertEqual(exported, 2)
        self.assertTrue(connection.closed)
        self.assertEqual(table["result_ldl_c"].to_pylist(), [0, 1])
        self.assertEqual(table["result_ua"].to_pylist(), [1, None])
        self.assertEqual(table.schema.field("result_ldl_c").type, pa.int8())
        self.assertEqual(table.schema.names[-3:], ["result_ldl_c", "result_ua", "split"])

    def test_invalid_mapping(self):
        with self.assertRaisesRegex(Exception, "表名=输出字段名"):
            parse_item_mapping("lab_item_tc")


if __name__ == "__main__":
    unittest.main()
