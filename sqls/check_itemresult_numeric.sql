-- Read-only check: determine whether every itemresult can be converted to
-- DOUBLE without silently coercing non-numeric text.
--
-- Default target: lab_item_tc
-- To check another table in the same mysql CLI session:
--   SET @target_table = 'lab_item_tc';
--   SOURCE /path/to/sqls/check_itemresult_numeric.sql;

USE `datas_in_progress`;

SET @target_table = COALESCE(@target_table, 'lab_item_tc');
SET @quoted_target_table = CONCAT(
    '`', REPLACE(@target_table, '`', '``'), '`'
);

-- Accepted forms include 12, -12, +12.5, .5, 5., 1e3, and -1.2E-3,
-- optionally surrounded by whitespace.
SET @numeric_pattern =
    '^[[:space:]]*[+-]?(([0-9]+([.][0-9]*)?)|([.][0-9]+))([eE][+-]?[0-9]+)?[[:space:]]*$';

-- Overall result. non_convertible_rows includes NULL, blank strings, and
-- non-numeric text. It must be 0 before converting to DOUBLE NOT NULL.
SET @sql = CONCAT(
    'SELECT ',
    'COUNT(*) AS total_rows, ',
    'SUM(`itemresult` IS NULL) AS null_rows, ',
    'SUM(`itemresult` IS NOT NULL ',
        'AND TRIM(CAST(`itemresult` AS CHAR)) = '''') AS blank_rows, ',
    'SUM(`itemresult` IS NOT NULL ',
        'AND REGEXP_LIKE(CAST(`itemresult` AS CHAR), ',
        QUOTE(@numeric_pattern), ')) AS numeric_rows, ',
    'SUM(`itemresult` IS NULL ',
        'OR NOT REGEXP_LIKE(CAST(`itemresult` AS CHAR), ',
        QUOTE(@numeric_pattern), ')) AS non_convertible_rows ',
    'FROM ', @quoted_target_table
);
PREPARE check_stmt FROM @sql;
EXECUTE check_stmt;
DEALLOCATE PREPARE check_stmt;

-- Show each invalid value and its frequency. NULL appears as its own group.
SET @sql = CONCAT(
    'SELECT `itemresult`, COUNT(*) AS rows_count ',
    'FROM ', @quoted_target_table, ' ',
    'WHERE `itemresult` IS NULL ',
       'OR NOT REGEXP_LIKE(CAST(`itemresult` AS CHAR), ',
       QUOTE(@numeric_pattern), ') ',
    'GROUP BY `itemresult` ',
    'ORDER BY rows_count DESC, `itemresult` ',
    'LIMIT 100'
);
PREPARE check_stmt FROM @sql;
EXECUTE check_stmt;
DEALLOCATE PREPARE check_stmt;

-- Show up to 100 affected records for source-data inspection.
SET @sql = CONCAT(
    'SELECT `uid`, `id`, `itemresult` ',
    'FROM ', @quoted_target_table, ' ',
    'WHERE `itemresult` IS NULL ',
       'OR NOT REGEXP_LIKE(CAST(`itemresult` AS CHAR), ',
       QUOTE(@numeric_pattern), ') ',
    'LIMIT 100'
);
PREPARE check_stmt FROM @sql;
EXECUTE check_stmt;
DEALLOCATE PREPARE check_stmt;

SET @target_table = NULL;
SET @quoted_target_table = NULL;
SET @numeric_pattern = NULL;
SET @sql = NULL;
