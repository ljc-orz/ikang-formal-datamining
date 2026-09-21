# 体检指标 Parquet 提取与 MySQL 导入

本项目把原始体检明细按 `items.csv` 中的项目拆分，再按照参考队列中的
`(uid, id)` 精确筛选，最后可将任意一个筛选前或筛选后的 Parquet 流式导入
MySQL。三个步骤都按批处理数据，不需要把大型明细文件整体载入内存。

## 项目结构

```text
ikang-formal-datamining/
├── src/
│   ├── export_items.py
│   ├── export_split_to_parquet_extra.py
│   ├── filter_parquets_by_ids.py
│   └── import_parquet_to_mysql.py
├── tests/
│   ├── test_export_items.py
│   ├── test_export_split_to_parquet_extra.py
│   ├── test_filter_parquets_by_ids.py
│   └── test_import_parquet_to_mysql.py
├── sqls/
│   ├── check_itemresult_numeric.sql
│   ├── check_normal_values_numeric.sql
│   ├── migrate_itemindexresultunit_to_text.sql
│   ├── migrate_itemresult_to_double.sql
│   ├── migrate_normal_values_to_double.sql
│   └── migrate_lab_item_uid_to_itemname.sql
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

## 3. 合并额外指标并导出划分 Parquet

`src/export_split_to_parquet_extra.py` 以
`datas.result_merged_wide_split_50000` 为主表，通过 `(uid, id)` 内连接
`datas_in_progress` 中的额外指标表。两个导出程序的非结果字段完全一致，均为
`uid`、`id`、`hospid`、`usex`、`examage`、两列图像路径以及来自 `datas` 主表的
`split`。结果字段则不再导出主表原有的 `result_alt` 到 `result_wbc`，只导出
`--item` 配置的额外结果字段。每张指标表按照含边界的正常范围生成标签：
`itemresult`、`normallowvalue` 或 `normalhighvalue` 任一为 NULL 时输出 NULL；
`normallowvalue <= itemresult <= normalhighvalue` 时输出 0（正常），否则输出
1（异常）。动态结果字段与原始导出程序一致，使用 int8 类型并写在 `split` 之前。

表名和输出字段名通过可重复的 `--item TABLE=OUTPUT_FIELD` 参数配置。导出当前五个
指标的命令为：

```bash
conda run --no-capture-output -n ikang python src/export_split_to_parquet_extra.py \
  --item lab_item_ldl_c=result_ldl_c \
  --item lab_item_plt=result_plt \
  --item lab_item_tc=result_tc \
  --item lab_item_ua=result_ua \
  --item lab_item_urea=result_urea \
  --output /DaTa/ljc_codes/ikang-formal/prepare_data/result_merged_wide_split_50000_extra.parquet \
  --expected-rows 50000 \
  --batch-size 5000
```

程序使用服务端游标流式读取并显示 Rich 进度条，按 `uid,id` 排序输出。只有导出行数
和 Parquet 元数据均通过检查后才会原子替换最终文件；默认拒绝覆盖，重新导出需增加
`--overwrite`。以后增加或更换指标时只需调整 `--item`，代码中没有硬编码这五个
字段。

## 4. 导入单个 Parquet 到 MySQL

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

## 5. 调整 `lab_item_alt` 的前半段表结构

`sqls/migrate_lab_item_uid_to_itemname.sql` 将 `uid` 到 `itemname` 修改为目标
类型，并创建 `(uid, id)` 主键以及 `hospid`、`regdate`、`usex`、
`checkitemcode`、`itemcode` 索引。迁移时会完成以下数据规范化：

- `uid`、`id` 去除首尾空白；
- `hospid`、`regdate`、`deptid` 的空字符串改为 `NULL`；
- 原始性别 `男` 映射为枚举值 `MAN`；
- 原始性别 `女` 映射为枚举值 `WOMAN`；
- 其他性别值和空值映射为 `UNK`。
- `examage` 的空值映射为 `0.0`。

MySQL 的 DDL 会隐式提交，因此 `UPDATE` 和 `ALTER TABLE` 不能组成一个失败后完整
回滚的事务。该脚本不直接修改原表，而是创建并填充 `表名__new` 影子表，确认行数
一致后用一条原子 `RENAME TABLE` 完成切换。迁移失败时原表保持不变；迁移成功后，
原始数据保留在 `表名__backup`，确认结果后再手工删除备份。

执行命令：

```bash
mysql \
  --socket=/DaTa/mysql/mysql.sock \
  --user=root --password=root \
  datas_in_progress \
  < sqls/migrate_lab_item_uid_to_itemname.sql
```

在 MySQL CLI 中默认迁移 `lab_item_tc`：

```sql
SOURCE /DaTa/ljc_codes/ikang-formal-datamining/sqls/migrate_lab_item_uid_to_itemname.sql;
```

迁移其他表时先设置表名。例如：

```sql
SET @target_table = 'lab_item_tc';
SOURCE /DaTa/ljc_codes/ikang-formal-datamining/sqls/migrate_lab_item_uid_to_itemname.sql;
```

如果 `表名__backup` 已存在，脚本会拒绝执行，避免覆盖原始备份。非空但无法转换
的日期或数值也会中止迁移，原表仍保持不变。

在把 `itemresult` 修改为 `DOUBLE` 前，可先运行只读检查：

```sql
SOURCE /DaTa/ljc_codes/ikang-formal-datamining/sqls/check_itemresult_numeric.sql;
```

检查其他表时先设置 `@target_table`。结果中的 `non_convertible_rows` 包含 NULL、
空字符串和非数字文本；只有该值为 0 时，所有 `itemresult` 才能直接转换为
`DOUBLE NOT NULL`。脚本还会按频次列出最多100种异常值及100条对应记录。

确认 `non_convertible_rows = 0` 后，将 `itemresult` 转为 `DOUBLE NOT NULL`：

```sql
SOURCE /DaTa/ljc_codes/ikang-formal-datamining/sqls/migrate_itemresult_to_double.sql;
```

该脚本默认修改 `lab_item_tc`，并且只执行一条 `ALTER TABLE`。MySQL 8/InnoDB 会
原子应用这条 DDL；若某个值超出 DOUBLE 范围等原因导致失败，原字段和值保持不变。

检查 `normallowvalue` 和 `normalhighvalue` 是否能转换为 nullable DOUBLE：

```sql
SOURCE /DaTa/ljc_codes/ikang-formal-datamining/sqls/check_normal_values_numeric.sql;
```

脚本默认检查 `lab_item_tc`。NULL 对目标 `DOUBLE NULL` 是合法值，不计入
`non_convertible_rows`；空字符串和非数字文本会计入，并在后续结果中按频次列出。

确认两列的 `non_convertible_rows` 都为 0 后执行转换：

```sql
SOURCE /DaTa/ljc_codes/ikang-formal-datamining/sqls/migrate_normal_values_to_double.sql;
```

脚本通过同一条 `ALTER TABLE` 把 `normallowvalue` 和 `normalhighvalue` 改为
`DOUBLE NULL`。两列转换会一起成功；如果任一列转换失败，原字段和值保持不变。

最后把结果单位改为目标 nullable TEXT：

```sql
SOURCE /DaTa/ljc_codes/ikang-formal-datamining/sqls/migrate_itemindexresultunit_to_text.sql;
```

该脚本默认把 `lab_item_tc.itemindexresultunit` 修改为 `TEXT NULL`。如果某个值超过
TEXT 容量，单条原子 DDL 会失败，原字段定义和值保持不变。

## 测试

测试会在临时目录动态生成小型 Parquet。MySQL 测试使用假连接验证建表 SQL、批量
参数和事务调用，不会连接或修改真实数据库：

```bash
PYTHONPATH=src conda run -n ikang python -m unittest discover -s tests -v
```

真实数据和 MySQL 的连通性可先选择一个零行或很小的筛选结果做 smoke test；确认
表结构和字符编码无误后再导入大文件。
