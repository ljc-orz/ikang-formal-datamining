-- Read-only check for normallowvalue and normalhighvalue before converting
-- them to nullable DOUBLE columns.
--
-- Default target: lab_item_tc
-- To check another table in the same mysql CLI session:
--   SET @target_table = 'lab_item_alt';
--   SOURCE /path/to/sqls/check_normal_values_numeric.sql;

USE `datas_in_progress`;

SET @target_table = COALESCE(@target_table, 'lab_item_tc');
SET @quoted_target_table = CONCAT(
    '`', REPLACE(@target_table, '`', '``'), '`'
);

-- Accepted forms include 12, -12, +12.5, .5, 5., 1e3, and -1.2E-3,
-- optionally surrounded by whitespace.
SET @numeric_pattern =
    '^[[:space:]]*[+-]?(([0-9]+([.][0-9]*)?)|([.][0-9]+))([eE][+-]?[0-9]+)?[[:space:]]*$';

-- NULL is valid for the target DOUBLE NULL columns. non_convertible_rows only
-- counts non-NULL blank strings and non-numeric text, and must be 0 before a
-- direct type conversion.
SET @sql = CONCAT(
    'SELECT ''normallowvalue'' AS column_name, ',
    'COUNT(*) AS total_rows, ',
    'SUM(`normallowvalue` IS NULL) AS null_rows, ',
    'SUM(`normallowvalue` IS NOT NULL ',
        'AND TRIM(CAST(`normallowvalue` AS CHAR)) = '''') AS blank_rows, ',
    'SUM(`normallowvalue` IS NOT NULL ',
        'AND REGEXP_LIKE(CAST(`normallowvalue` AS CHAR), ',
        QUOTE(@numeric_pattern), ')) AS numeric_rows, ',
    'SUM(`normallowvalue` IS NOT NULL ',
        'AND NOT REGEXP_LIKE(CAST(`normallowvalue` AS CHAR), ',
        QUOTE(@numeric_pattern), ')) AS non_convertible_rows ',
    'FROM ', @quoted_target_table, ' ',
    'UNION ALL ',
    'SELECT ''normalhighvalue'' AS column_name, ',
    'COUNT(*) AS total_rows, ',
    'SUM(`normalhighvalue` IS NULL) AS null_rows, ',
    'SUM(`normalhighvalue` IS NOT NULL ',
        'AND TRIM(CAST(`normalhighvalue` AS CHAR)) = '''') AS blank_rows, ',
    'SUM(`normalhighvalue` IS NOT NULL ',
        'AND REGEXP_LIKE(CAST(`normalhighvalue` AS CHAR), ',
        QUOTE(@numeric_pattern), ')) AS numeric_rows, ',
    'SUM(`normalhighvalue` IS NOT NULL ',
        'AND NOT REGEXP_LIKE(CAST(`normalhighvalue` AS CHAR), ',
        QUOTE(@numeric_pattern), ')) AS non_convertible_rows ',
    'FROM ', @quoted_target_table
);
PREPARE check_stmt FROM @sql;
EXECUTE check_stmt;
DEALLOCATE PREPARE check_stmt;

-- List invalid non-NULL values from both columns, ordered by frequency.
SET @sql = CONCAT(
    'SELECT `column_name`, `invalid_value`, `rows_count` FROM (',
        'SELECT ''normallowvalue'' AS column_name, ',
        'CAST(`normallowvalue` AS CHAR) AS invalid_value, ',
        'COUNT(*) AS rows_count ',
        'FROM ', @quoted_target_table, ' ',
        'WHERE `normallowvalue` IS NOT NULL ',
          'AND NOT REGEXP_LIKE(CAST(`normallowvalue` AS CHAR), ',
          QUOTE(@numeric_pattern), ') ',
        'GROUP BY `normallowvalue` ',
        'UNION ALL ',
        'SELECT ''normalhighvalue'' AS column_name, ',
        'CAST(`normalhighvalue` AS CHAR) AS invalid_value, ',
        'COUNT(*) AS rows_count ',
        'FROM ', @quoted_target_table, ' ',
        'WHERE `normalhighvalue` IS NOT NULL ',
          'AND NOT REGEXP_LIKE(CAST(`normalhighvalue` AS CHAR), ',
          QUOTE(@numeric_pattern), ') ',
        'GROUP BY `normalhighvalue`',
    ') AS invalid_values ',
    'ORDER BY rows_count DESC, column_name, invalid_value ',
    'LIMIT 200'
);
PREPARE check_stmt FROM @sql;
EXECUTE check_stmt;
DEALLOCATE PREPARE check_stmt;

-- Show up to 200 affected records across both columns.
SET @sql = CONCAT(
    'SELECT ''normallowvalue'' AS column_name, `uid`, `id`, ',
    'CAST(`normallowvalue` AS CHAR) AS invalid_value ',
    'FROM ', @quoted_target_table, ' ',
    'WHERE `normallowvalue` IS NOT NULL ',
      'AND NOT REGEXP_LIKE(CAST(`normallowvalue` AS CHAR), ',
      QUOTE(@numeric_pattern), ') ',
    'UNION ALL ',
    'SELECT ''normalhighvalue'' AS column_name, `uid`, `id`, ',
    'CAST(`normalhighvalue` AS CHAR) AS invalid_value ',
    'FROM ', @quoted_target_table, ' ',
    'WHERE `normalhighvalue` IS NOT NULL ',
      'AND NOT REGEXP_LIKE(CAST(`normalhighvalue` AS CHAR), ',
      QUOTE(@numeric_pattern), ') ',
    'LIMIT 200'
);
PREPARE check_stmt FROM @sql;
EXECUTE check_stmt;
DEALLOCATE PREPARE check_stmt;

SET @target_table = NULL;
SET @quoted_target_table = NULL;
SET @numeric_pattern = NULL;
SET @sql = NULL;
