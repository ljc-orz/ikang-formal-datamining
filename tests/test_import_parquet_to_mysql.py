import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from import_parquet_to_mysql import create_table_sql, run_import


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = []

    def execute(self, sql, params=None):
        self.connection.executed.append((sql, params))
        if sql.startswith("SELECT 1 FROM information_schema.tables"):
            self.result = [(1,)] if self.connection.table_exists else []
        elif sql.startswith("SHOW COLUMNS"):
            self.result = [(name,) for name in self.connection.columns]
        elif sql.startswith("DROP TABLE"):
            self.connection.table_exists = False
        elif sql.startswith("CREATE TABLE"):
            self.connection.table_exists = True
        return 1

    def executemany(self, sql, rows):
        rows = list(rows)
        self.connection.insert_sql = sql
        self.connection.rows.extend(rows)
        return len(rows)

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result

    def close(self):
        self.connection.cursor_closed = True


class FakeConnection:
    def __init__(self, table_exists=False, columns=None):
        self.table_exists = table_exists
        self.columns = columns or []
        self.executed = []
        self.rows = []
        self.insert_sql = ""
        self.commits = 0
        self.rollbacks = 0
        self.cursor_closed = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class ImportParquetToMySQLTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.parquet = self.root / "input.parquet"
        self.schema = pa.schema(
            [
                pa.field("uid", pa.string()),
                pa.field("id", pa.string()),
                pa.field("value", pa.string()),
                pa.field("count", pa.int64()),
                pa.field("score", pa.float64()),
                pa.field("active", pa.bool_()),
            ]
        )
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "uid": "u1",
                        "id": "i1",
                        "value": "中文",
                        "count": 1,
                        "score": 0.5,
                        "active": True,
                    },
                    {
                        "uid": "u2",
                        "id": "i2",
                        "value": None,
                        "count": 2,
                        "score": float("nan"),
                        "active": False,
                    },
                ],
                schema=self.schema,
            ),
            self.parquet,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_creates_table_and_inserts_in_batches(self):
        connection = FakeConnection()
        inserted = run_import(
            parquet_path=self.parquet,
            table_name="test_results",
            batch_size=1,
            connection_factory=lambda: connection,
        )

        self.assertEqual(inserted, 2)
        create_statements = [
            sql for sql, _ in connection.executed if sql.startswith("CREATE TABLE")
        ]
        self.assertEqual(len(create_statements), 1)
        ddl = create_statements[0]
        self.assertIn("`uid` LONGTEXT", ddl)
        self.assertIn("`count` BIGINT", ddl)
        self.assertIn("`score` DOUBLE", ddl)
        self.assertIn("`active` TINYINT(1)", ddl)
        self.assertEqual(connection.rows[0][:3], ("u1", "i1", "中文"))
        self.assertIsNone(connection.rows[1][2])
        self.assertIsNone(connection.rows[1][4])
        self.assertEqual(connection.commits, 2)
        self.assertTrue(connection.cursor_closed)
        self.assertTrue(connection.closed)

    def test_existing_table_fails_by_default(self):
        connection = FakeConnection(table_exists=True)
        with self.assertRaises(FileExistsError):
            run_import(
                parquet_path=self.parquet,
                table_name="test_results",
                connection_factory=lambda: connection,
            )
        self.assertEqual(connection.rows, [])
        self.assertEqual(connection.rollbacks, 1)

    def test_append_validates_column_order(self):
        connection = FakeConnection(
            table_exists=True,
            columns=self.schema.names,
        )
        inserted = run_import(
            parquet_path=self.parquet,
            table_name="test_results",
            if_exists="append",
            connection_factory=lambda: connection,
        )
        self.assertEqual(inserted, 2)
        self.assertFalse(
            any(sql.startswith("CREATE TABLE") for sql, _ in connection.executed)
        )

    def test_create_table_rejects_nested_arrow_type(self):
        nested_schema = pa.schema([pa.field("values", pa.list_(pa.int64()))])
        with self.assertRaisesRegex(TypeError, "unsupported Arrow type"):
            create_table_sql("nested", nested_schema)


if __name__ == "__main__":
    unittest.main()
