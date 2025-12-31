-- ----------------------------------------------------------
-- EXOSKELETON STORED PROCEDURES FOR MARIADB
-- for version 3.0.0+ of exoskeleton (Post ORM Migration)
-- © 2019-2025 Rüdiger Voigt and contributors
-- APACHE-2 LICENSE
--
-- Summary:
-- This script creates 3 stored procedures
-- Tables are auto-created by SQLAlchemy from models.py.
-- Schema validation uses SQLAlchemy Inspector
--
--
-- NOTE: Database name is specified by caller (e.g., mysql dbname < file.sql)
-- No USE statement needed in that case - procedures are created in the active database

-- ----------------------------------------------------------
-- FILE MANAGEMENT PROCEDURES (3)
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

-- insert_file_SP:
-- Save file metadata, create version entry, remove from queue.
-- Complex transaction with error handling - better in database.
DELIMITER $$
CREATE PROCEDURE insert_file_SP (IN url_p TEXT,
                                 IN url_hash_p CHAR(64),
                                 IN queueID_p CHAR(32) CHARACTER SET ASCII,
                                 IN mimeType_p VARCHAR(127),
                                 IN path_or_bucket_p VARCHAR(2048),
                                 IN file_name_p VARCHAR(255),
                                 IN size_p INT UNSIGNED,
                                 IN hash_method_p VARCHAR(6),
                                 IN hash_value_p VARCHAR(512),
                                 IN actionAppliedID_p TINYINT UNSIGNED
                                 )
MODIFIES SQL DATA
BEGIN

DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
        ROLLBACK;
        UPDATE queue SET causesError = 2 WHERE id = queueID_p;
        RESIGNAL;
    END;

    START TRANSACTION;

    INSERT IGNORE INTO fileMaster (url, urlHash) VALUES (url_p, url_hash_p);

    SELECT id FROM fileMaster WHERE urlHash = url_hash_p INTO @fileMasterID;

    INSERT INTO fileVersions (id, fileMasterID, storageTypeID, mimeType,
                              pathOrBucket, fileName, size, hashMethod,
                              hashValue, actionAppliedID) VALUES (queueID_p, @fileMasterID, 2, mimeType_p,
                              path_or_bucket_p, file_name_p, size_p,
                              hash_method_p, hash_value_p, actionAppliedID_p);

    DELETE FROM queue WHERE id = queueID_p;

    COMMIT;

END $$
DELIMITER ;

-- insert_content_SP:
-- Save page content to database, create version entry, remove from queue.
-- Complex transaction with error handling - better in database.
DELIMITER $$
CREATE PROCEDURE insert_content_SP (IN url_p TEXT,
                                    IN url_hash_p CHAR(64),
                                    IN queueID_p CHAR(32) CHARACTER SET ASCII,
                                    IN mimeType_p VARCHAR(127),
                                    IN text_p MEDIUMTEXT,
                                    IN actionAppliedID_p TINYINT UNSIGNED)
MODIFIES SQL DATA
BEGIN

DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
        ROLLBACK;
        UPDATE queue SET causesError = 2 WHERE id = queueID_p;
        RESIGNAL;
    END;

    START TRANSACTION;

    INSERT IGNORE INTO fileMaster (url, urlHash) VALUES (url_p, url_hash_p);
    SELECT id FROM fileMaster WHERE urlHash = url_hash_p INTO @fileMasterID;

    INSERT INTO fileVersions (id, fileMasterID, storageTypeID, mimeType, actionAppliedID)
    VALUES (queueID_p, @fileMasterID, 1, mimeType_p, actionAppliedID_p);

    INSERT INTO fileContent (versionID, pageContent)
    VALUES (queueID_p, text_p);

    DELETE FROM queue WHERE id = queueID_p;

    COMMIT;
END $$
DELIMITER ;

-- ----------------------------------------------------------
-- END OF SCRIPT
-- ----------------------------------------------------------
