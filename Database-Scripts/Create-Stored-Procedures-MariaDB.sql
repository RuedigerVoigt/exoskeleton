-- ----------------------------------------------------------
-- EXOSKELETON STORED PROCEDURES FOR MARIADB
-- for version 3.0.0+ of exoskeleton (Post ORM Migration)
-- © 2019-2025 Rüdiger Voigt and contributors
-- APACHE-2 LICENSE
--
-- Summary:
-- This script creates 1 stored procedure
-- Tables are auto-created by SQLAlchemy from models.py.
-- Schema validation uses SQLAlchemy Inspector
--
--
-- NOTE: Database name is specified by caller (e.g., mysql dbname < file.sql)
-- No USE statement needed in that case - procedures are created in the active database

-- ----------------------------------------------------------
-- FILE MANAGEMENT PROCEDURES (1)
-- ----------------------------------------------------------

-- delete_all_versions_SP:
-- Delete all versions of a file including labels and the fileMaster entry.
-- Uses transaction to ensure consistency across multiple tables.
DELIMITER $$
CREATE PROCEDURE delete_all_versions_SP (IN fileMasterID_p INT)
MODIFIES SQL DATA
BEGIN

DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;
    -- remove all labels attached to versions:
    DELETE FROM labelToVersion WHERE versionUUID IN (
        SELECT id FROM fileVersions WHERE fileMasterID = fileMasterID_p
        );
    -- now as the CONSTRAINT does not interfere, remove all versions:
    DELETE FROM fileVersions WHERE fileMasterID = fileMasterID_p;

    -- remove all labels attached to the fileMaster:
    DELETE FROM labelToMaster WHERE urlHash = (
        SELECT urlHash FROM fileMaster WHERE id = fileMasterID_p
    );
    -- now as there are no versions and the label CONSTRAINT
    -- does not interfere, remove the entry in FileMaster:
    DELETE FROM fileMaster WHERE id = fileMasterID_p;
    COMMIT;

END $$
DELIMITER ;

-- ----------------------------------------------------------
-- END OF SCRIPT
-- ----------------------------------------------------------
