-- Convert normallowvalue and normalhighvalue to nullable DOUBLE columns.
-- Run check_normal_values_numeric.sql first and confirm that both columns have
-- non_convertible_rows = 0.
--
-- Default target: lab_item_tc
-- To convert another table in the same mysql CLI session:
--   SET @target_table = 'lab_item_alt';
--   SOURCE /path/to/sqls/migrate_normal_values_to_double.sql;

USE `datas_in_progress`;

SET @target_table = COALESCE(@target_table, 'lab_item_tc');
SET @quoted_target_table = CONCAT(
    '`', REPLACE(@target_table, '`', '``'), '`'
);

-- Both column changes are deliberately kept in one ALTER TABLE statement.
-- MySQL 8/InnoDB applies this DDL atomically: both conversions succeed, or
-- neither column definition and none of the values are changed.
SET @sql = CONCAT(
    'ALTER TABLE ', @quoted_target_table, ' ',
    'MODIFY COLUMN `normallowvalue` DOUBLE NULL, ',
    'MODIFY COLUMN `normalhighvalue` DOUBLE NULL'
);
PREPARE migration_stmt FROM @sql;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = @target_table
  AND column_name IN ('normallowvalue', 'normalhighvalue')
ORDER BY FIELD(column_name, 'normallowvalue', 'normalhighvalue');

SET @target_table = NULL;
SET @quoted_target_table = NULL;
SET @sql = NULL;
