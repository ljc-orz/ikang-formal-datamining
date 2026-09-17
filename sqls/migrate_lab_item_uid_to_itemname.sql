-- Safely migrate uid through itemname to the target MySQL schema.
--
-- MySQL DDL implicitly commits, so UPDATE + ALTER TABLE cannot be made one
-- rollback-capable transaction. This script instead builds and validates a
-- shadow table, then atomically swaps it with the source table. Until the
-- final RENAME TABLE succeeds, the source table is never modified.
--
-- Default target: lab_item_tc
-- To use another table in the same mysql CLI session:
--   SET @target_table = 'lab_item_tc';
--   SOURCE /path/to/sqls/migrate_lab_item_uid_to_itemname.sql;

USE `datas_in_progress`;

SET @target_table = COALESCE(@target_table, 'lab_item_tc');

DELIMITER $$

DROP PROCEDURE IF EXISTS `migrate_lab_item_uid_to_itemname`$$

CREATE PROCEDURE `migrate_lab_item_uid_to_itemname`(IN p_table VARCHAR(48))
BEGIN
    DECLARE v_shadow VARCHAR(64);
    DECLARE v_backup VARCHAR(64);
    DECLARE v_backup_exists BIGINT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    IF p_table IS NULL
       OR p_table = ''
       OR p_table NOT REGEXP '^[0-9A-Za-z_]+$' THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'target table must contain only letters, digits, and underscores';
    END IF;

    SET v_shadow = CONCAT(p_table, '__new');
    SET v_backup = CONCAT(p_table, '__backup');

    SELECT COUNT(*)
    INTO v_backup_exists
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name = v_backup;

    IF v_backup_exists > 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'backup table already exists; inspect or remove it before retrying';
    END IF;

    -- A leftover shadow table can only be an incomplete result from an earlier
    -- failed attempt. The live source table has not been changed at this point.
    SET @sql = CONCAT('DROP TABLE IF EXISTS `', v_shadow, '`');
    PREPARE migration_stmt FROM @sql;
    EXECUTE migration_stmt;
    DEALLOCATE PREPARE migration_stmt;

    SET @sql = CONCAT(
        'CREATE TABLE `', v_shadow, '` LIKE `', p_table, '`'
    );
    PREPARE migration_stmt FROM @sql;
    EXECUTE migration_stmt;
    DEALLOCATE PREPARE migration_stmt;

    -- The shadow table is empty, so this DDL cannot partially convert data.
    SET @sql = CONCAT(
        'ALTER TABLE `', v_shadow, '` ',
        'MODIFY COLUMN `uid` CHAR(32) NOT NULL, ',
        'MODIFY COLUMN `id` CHAR(18) NOT NULL, ',
        'MODIFY COLUMN `hospid` BIGINT NULL, ',
        'MODIFY COLUMN `hospname` TEXT NULL, ',
        'MODIFY COLUMN `regdate` DATE NULL, ',
        'MODIFY COLUMN `examage` DOUBLE NOT NULL, ',
        'MODIFY COLUMN `usex` ENUM(''MAN'', ''WOMAN'', ''UNK'') NOT NULL, ',
        'MODIFY COLUMN `deptid` DOUBLE NULL, ',
        'MODIFY COLUMN `dept` TEXT NULL, ',
        'MODIFY COLUMN `checkitemcode` TEXT NULL, ',
        'MODIFY COLUMN `checkitemname` TEXT NULL, ',
        'MODIFY COLUMN `itemcode` TEXT NULL, ',
        'MODIFY COLUMN `itemname` TEXT NULL, ',
        'ADD PRIMARY KEY (`uid`, `id`), ',
        'ADD INDEX `idx_hospid` (`hospid`), ',
        'ADD INDEX `idx_regdate` (`regdate`), ',
        'ADD INDEX `idx_usex` (`usex`), ',
        'ADD INDEX `idx_checkitemcode` (`checkitemcode`(32)), ',
        'ADD INDEX `idx_itemcode` (`itemcode`(32))'
    );
    PREPARE migration_stmt FROM @sql;
    EXECUTE migration_stmt;
    DEALLOCATE PREPARE migration_stmt;

    -- One INSERT statement performs all value conversion. InnoDB rolls the
    -- statement back if any row has an invalid value or duplicate (uid, id).
    -- Missing examage is explicitly represented as 0.0.
    START TRANSACTION;
    SET @sql = CONCAT(
        'INSERT INTO `', v_shadow, '` (',
        '`uid`, `id`, `hospid`, `hospname`, `regdate`, `examage`, `usex`, ',
        '`deptid`, `dept`, `checkitemcode`, `checkitemname`, `itemcode`, ',
        '`itemname`, `itemresult`, `normalhighvalue`, `normallowvalue`, ',
        '`itemindexresultunit`) ',
        'SELECT ',
        'TRIM(`uid`), ',
        'TRIM(`id`), ',
        'NULLIF(TRIM(CAST(`hospid` AS CHAR)), ''''), ',
        '`hospname`, ',
        'NULLIF(TRIM(CAST(`regdate` AS CHAR)), ''''), ',
        'COALESCE(`examage`, 0.0), ',
        'CASE ',
        'WHEN TRIM(CAST(`usex` AS CHAR)) IN (''男'', ''MAN'') THEN ''MAN'' ',
        'WHEN TRIM(CAST(`usex` AS CHAR)) IN (''女'', ''WOMAN'') THEN ''WOMAN'' ',
        'ELSE ''UNK'' END, ',
        'NULLIF(TRIM(CAST(`deptid` AS CHAR)), ''''), ',
        '`dept`, `checkitemcode`, `checkitemname`, `itemcode`, `itemname`, ',
        '`itemresult`, `normalhighvalue`, `normallowvalue`, ',
        '`itemindexresultunit` ',
        'FROM `', p_table, '`'
    );
    PREPARE migration_stmt FROM @sql;
    EXECUTE migration_stmt;
    DEALLOCATE PREPARE migration_stmt;
    COMMIT;

    SET @sql = CONCAT(
        'SELECT COUNT(*) INTO @source_row_count FROM `', p_table, '`'
    );
    PREPARE migration_stmt FROM @sql;
    EXECUTE migration_stmt;
    DEALLOCATE PREPARE migration_stmt;

    SET @sql = CONCAT(
        'SELECT COUNT(*) INTO @migrated_row_count FROM `', v_shadow, '`'
    );
    PREPARE migration_stmt FROM @sql;
    EXECUTE migration_stmt;
    DEALLOCATE PREPARE migration_stmt;

    IF @source_row_count <> @migrated_row_count THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'source and shadow table row counts differ';
    END IF;

    -- Multi-table RENAME is the only cutover: both renames succeed together or
    -- neither takes effect. The original data remains available as __backup.
    SET @sql = CONCAT(
        'RENAME TABLE `', p_table, '` TO `', v_backup, '`, ',
        '`', v_shadow, '` TO `', p_table, '`'
    );
    PREPARE migration_stmt FROM @sql;
    EXECUTE migration_stmt;
    DEALLOCATE PREPARE migration_stmt;

    SELECT
        p_table AS migrated_table,
        @migrated_row_count AS migrated_rows,
        v_backup AS original_backup;
END$$

DELIMITER ;

CALL `migrate_lab_item_uid_to_itemname`(@target_table);
DROP PROCEDURE IF EXISTS `migrate_lab_item_uid_to_itemname`;

-- Keep the session reusable: a later SOURCE defaults to lab_item_tc unless
-- the caller explicitly sets @target_table again.
SET @target_table = NULL;
