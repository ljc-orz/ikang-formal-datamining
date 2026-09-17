-- Convert itemresult to DOUBLE NOT NULL.
-- Run check_itemresult_numeric.sql first and confirm non_convertible_rows = 0.
--
-- Default target: lab_item_tc
-- To convert another table in the same mysql CLI session:
--   SET @target_table = 'lab_item_alt';
--   SOURCE /path/to/sqls/migrate_itemresult_to_double.sql;

USE `datas_in_progress`;

SET @target_table = COALESCE(@target_table, 'lab_item_tc');
SET @quoted_target_table = CONCAT(
    '`', REPLACE(@target_table, '`', '``'), '`'
);

-- This is deliberately one ALTER TABLE statement. MySQL 8/InnoDB applies the
-- DDL atomically: either every itemresult is converted or the table is left
-- with its original itemresult definition and values.
SET @sql = CONCAT(
    'ALTER TABLE ', @quoted_target_table, ' ',
    'MODIFY COLUMN `itemresult` DOUBLE NOT NULL'
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
  AND column_name = 'itemresult';

SET @target_table = NULL;
SET @quoted_target_table = NULL;
SET @sql = NULL;
