# 体检指标 Parquet 提取与 MySQL 导入

本项目把原始体检明细按 `items.csv` 中的项目拆分，再按照参考队列中的
`(uid, id)` 精确筛选，最后可将任意一个筛选前或筛选后的 Parquet 流式导入
MySQL。三个步骤都按批处理数据，不需要把大型明细文件整体载入内存。

## 项目结构

```text
ikang-formal-datamining/
├── src/
│   ├── export_items.py
│   ├── filter_parquets_by_ids.py
│   └── import_parquet_to_mysql.py
├── tests/
│   ├── test_export_items.py
│   ├── test_filter_parquets_by_ids.py
│   └── test_import_parquet_to_mysql.py
├── items.csv
└── README.md
```

## 环境

已有脚本以 `ikang` Conda 环境中的 Python 3.9、PyArrow 21 和 Rich 为基准。
MySQL 导入额外需要 PyMySQL；当前环境尚未安装时执行：

```bash
conda run -n ikang python -m pip install PyMySQL
```

使用 `conda run` 时建议增加 `--no-capture-output`，以便实时显示 Rich 进度条。

## 1. 按指标拆分原始 Parquet

`src/export_items.py` 扫描输入目录中的所有 `.parq` 文件，按 `items.csv` 的
`itemcode` 生成独立 Parquet。bytes 列解码为 UTF-8，`rn` 被删除，三个结果字段
保持为 nullable string。并行提取会先写 Snappy 临时分片，再合并为 Zstandard
文件，因此临时目录需要容纳一份筛选后的数据。

```bash
conda run --no-capture-output -n ikang python src/export_items.py \
  --input-dir /DaTa/20231129/zsyk2zhibiao20231128 \
  --items-csv items.csv \
  --output-dir /DaTa/20231129/zsyk2zhibiao20231128_by_item \
  --workers 8
```

磁盘空间不足时可用 `--staging-dir` 把临时分片放到其他磁盘。输出目录包含每个
项目的 Parquet 和 `export_summary.csv`；无匹配记录的项目仍会生成零行文件。

## 2. 按参考患者筛选

`src/filter_parquets_by_ids.py` 从参考文件加载唯一的非空 `(uid, id)` 组合，并行过滤
上一步的每个 Parquet。uid 和 id 作为复合键匹配，不会把分别出现但组合错误的
两列误认为同一患者。

```bash
conda run --no-capture-output -n ikang python src/filter_parquets_by_ids.py \
  --source-dir /DaTa/20231129/zsyk2zhibiao20231128_by_item \
  --reference-parquet /DaTa/ljc_codes/ikang-formal/prepare_data/result_merged_wide_split_50000.parquet \
  --output-dir /DaTa/20231129/zsyk2zhibiao20231128_by_item_filtered \
  --workers 8
```

输出文件保持源文件名、schema 和行序。`filter_summary.csv` 包含源行数与保留行数，
并按照 `retained_rows` 降序排列。

## 3. 导入单个 Parquet 到 MySQL

`src/import_parquet_to_mysql.py` 使用 PyMySQL 连接以下数据库：

```python
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
```

选择第二步输出中的一个文件导入：

```bash
conda run --no-capture-output -n ikang python src/import_parquet_to_mysql.py \
  --parquet /DaTa/20231129/zsyk2zhibiao20231128_by_item_filtered/血清镁__2.1.4.81.parquet \
  --table serum_magnesium \
  --batch-size 5000
```

不指定 `--table` 时，程序从 Parquet 文件名生成表名。程序根据 Arrow schema 自动
建表，逐批执行参数化 `INSERT`，每批提交一次并显示导入进度。常用类型映射为：

| Arrow 类型 | MySQL 类型 |
| --- | --- |
| string / large_string | LONGTEXT |
| binary / large_binary | LONGBLOB |
| int8 / int16 / int32 / int64 | TINYINT / SMALLINT / INT / BIGINT |
| float32 / float64 | FLOAT / DOUBLE |
| boolean | TINYINT(1) |
| decimal | DECIMAL |
| date / timestamp / time | DATE / DATETIME(6) / TIME(6) |

list、struct、map 等嵌套类型不会被隐式序列化，遇到时直接报错。`None` 写为 SQL
`NULL`，非有限浮点值也写为 `NULL`。脚本不会自动创建索引或主键，因此源文件中的
重复行会原样导入。

同名表的默认行为是拒绝写入：

- `--if-exists fail`：默认值，表已存在时退出。
- `--if-exists append`：验证列名和顺序一致后追加。
- `--if-exists replace`：删除同名表，按当前 Parquet schema 重建后导入。

每批数据独立提交；如果中途失败，已经提交的批次会保留，错误批次会回滚。再次
导入前应根据实际情况删除该表、使用 `replace`，或确认不会产生重复后再使用
`append`。

## 测试

测试会在临时目录动态生成小型 Parquet。MySQL 测试使用假连接验证建表 SQL、批量
参数和事务调用，不会连接或修改真实数据库：

```bash
PYTHONPATH=src conda run -n ikang python -m unittest discover -s tests -v
```

真实数据和 MySQL 的连通性可先选择一个零行或很小的筛选结果做 smoke test；确认
表结构和字符编码无误后再导入大文件。
