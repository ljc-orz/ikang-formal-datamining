-- Convert itemindexresultunit to the target nullable TEXT type.
--
-- Default target: lab_item_tc
-- To convert another table in the same mysql CLI session:
--   SET @target_table = 'lab_item_alt';
--   SOURCE /path/to/sqls/migrate_itemindexresultunit_to_text.sql;

USE `datas_in_progress`;

SET @target_table = COALESCE(@target_table, 'lab_item_tc');
SET @quoted_target_table = CONCAT(
    '`', REPLACE(@target_table, '`', '``'), '`'
);

-- MySQL 8/InnoDB applies this single DDL statement atomically. If any value
-- exceeds the TEXT capacity, the original column definition and values remain.
SET @sql = CONCAT(
    'ALTER TABLE ', @quoted_target_table, ' ',
    'MODIFY COLUMN `itemindexresultunit` TEXT NULL'
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
  AND column_name = 'itemindexresultunit';

SET @target_table = NULL;
SET @quoted_target_table = NULL;
SET @sql = NULL;
