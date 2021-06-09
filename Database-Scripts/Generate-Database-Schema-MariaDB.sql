-- ----------------------------------------------------------
-- EXOSKELETON TABLE STRUCTURE FOR MARIADB
-- for version 1.4.0 of exoskeleton
-- © 2019-2021 Rüdiger Voigt
-- APACHE-2 LICENSE
--
-- This file generates the table structure needed for the
-- exoskeleton framework in MariaDB.
--
-- For the current version and a documentation of the package,
-- please see:
-- https://github.com/RuedigerVoigt/exoskeleton
--
-- BEWARE: This should be run in an empty database as otherwise
-- this script might alter existing tables!
--
-- ----------------------------------------------------------
-- SQL CONVENTIONS:
-- * Use US, not British spelling.
-- * Use mixedCase (lower camel case) for table- and field-names.
-- * Use snake case for the names of views, functions and stored procedures.
--   Use the prefix 'v_' for views and the suffix '_SP' for stored procedures.
--   Neither a prefix or a suffix for functions.
-- * Parameters of functions and procedures shall have the suffix '_p'.
-- ----------------------------------------------------------

-- DO NOT FORGET TO CHANGE:
CREATE DATABASE IF NOT EXISTS `exoskeleton` DEFAULT CHARACTER SET utf8mb4;
USE `exoskeleton`;

-- ----------------------------------------------------------
-- QUEUE
--
-- Needed by the bot to know what to do next.
--
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS queue (
    -- ID contains an UUID generated by Python instead of
    -- autoincrement, because:
    -- * After server restart the autoincrement counter is reset to
    --   MAX() which can lead to overwriting of files.
    -- * MariaDB generates different strings dependent on the OS.
    --   Python is RFC 4122 compliant and can be configured NOT to
    --   incorporate the machine's MAC address.
    -- * This spares a roundtrip to the DBMS as for compatibility
    --   reasons this cannot use INSERT .. RETURN and the script
    --   would have to query for the id using the URL.
    id CHAR(32) CHARACTER SET ASCII NOT NULL
    ,action TINYINT UNSIGNED
    -- MySQL does not no the alias BOOLEAN, so stick to TINYINT:
    ,prettifyHtml TINYINT UNSIGNED DEFAULT 0
    ,url TEXT NOT NULL
    ,urlHash CHAR(64) NOT NULL
    ,fqdnHash CHAR(64) NOT NULL
    ,addedToQueue TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,causesError INT NULL
    ,numTries INT DEFAULT 0
    ,delayUntil TIMESTAMP NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`action`)
    ,INDEX(`urlHash`)
    ,INDEX(`addedToQueue`)
    ,INDEX(`delayUntil`)
) ENGINE=InnoDB;


-- ----------------------------------------------------------
-- JOBS
--
-- Jobs are a tool to traverse multiple search result pages.
-- The named job holds the current URL so that the crawl must not be
-- restarted if it is stopped in between for any reason.
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS jobs (
    jobName VARCHAR(127) NOT NULL
    ,created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,finished TIMESTAMP NULL
    ,startUrl TEXT NOT NULL
    ,startUrlHash CHAR(64) NOT NULL
    ,currentUrl TEXT NULL
    ,PRIMARY KEY(`jobName`)
    ,INDEX(`created`)
    ,INDEX(`finished`)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- ACTIONS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS actions (
    id TINYINT UNSIGNED NOT NULL
    ,description VARCHAR(256) NOT NULL
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;

INSERT INTO actions (id, description) VALUES
(0, 'custom'),
(1, 'download file to disk'),
(2, 'save page code into database'),
(3, 'save as PDF using headless Chrome'),
(4, 'save text'),
(5, 'reserved for future use'),
(6, 'reserved for future use'),
(7, 'reserved for future use');

ALTER TABLE `queue`
ADD CONSTRAINT `actions`
FOREIGN KEY (`action`)
REFERENCES `actions`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

-- ----------------------------------------------------------
-- ERRORS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS errorType (
    id INT NOT NULL
    ,short VARCHAR(31)
    ,description VARCHAR(255)
    ,permanent BOOLEAN
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;

INSERT INTO errorType (id, short, permanent, description) VALUES
(0, 'unspecified exception', 0, 'see logfile'),
(1, 'malformed url', 1, 'URL is malformed. Missing Schema (http:// or https://) ?'),
(2, 'transaction fails', 0, 'The database transaction failed. Rollback was initiated.'),
(3, 'exceeded number of retries', 1, 'Tried this task the configured number of times with increasing time between tries.'),
(4, 'request timeout', 0, 'Did not get a reply from the server within the specified timeframe.'),
(5, 'process error', 0, ''),
(400, '400', 1, ''),
(401, '401', 1, ''),
(402, '402', 1, 'Server replied: Payment Required'),
(403, '403: Forbidden', 1, 'Server replied: Forbidden'),
(404, '404: File Not Found', 1, 'Server replied: File not found'),
(405, '405', 1, 'Server replied: Method Not Allowed'),
(406, '406', 1, ''),
(407, '407', 1, ''),
(408, '408', 0, ''),
(410, '410: Gone', 1, 'Server replied: Gone'),
(414, '414', 1, 'Server replied: URI too long'),
(429, '429: Rate Limit', 0, 'Server replied: Too Many Requests'),
(451, '451', 1, 'Server replied: Unavailable For Legal Reasons'),
(500, '500', 0, 'Server replied: Internal Server Error'),
(501, '501', 1, ''),
(502, '502', 0, ''),
(503, '503', 0, 'Server replied: Service Unavailable'),
(504, '504', 0, ''),
(509, '509', 0, ''),
(529, '529', 0, ''),
(598, '598', 0, '');

ALTER TABLE `queue`
ADD CONSTRAINT `errors`
FOREIGN KEY (`causesError`)
REFERENCES `errorType`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;


-- ----------------------------------------------------------
-- FILE STORAGE TYPES
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS storageTypes (
    id INT UNSIGNED NOT NULL
    ,shortName VARCHAR(15)
    ,fullName VARCHAR(63)
    ,PRIMARY KEY(`id`)
    ) ENGINE=InnoDB;

INSERT INTO storageTypes (id, shortName, fullName) VALUES
(1, 'Database', 'Local Database'),
(2, 'Filesystem', 'Local Filesystem'),
(3, 'AWS', 'Amazon Web Services'),
(4, 'GCP', 'Google Cloud Platform'),
(5, 'Azure', 'Microsoft Azure'),
(6, 'Alibaba', 'Alibaba');


-- ----------------------------------------------------------
-- FILE MASTER
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS fileMaster (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,url TEXT NOT NULL
    ,urlHash CHAR(64) NOT NULL
    ,numVersions_t INT DEFAULT 0
    -- increment / decrement of version count via trigger in fileVersions
    -- Default has to be 0 not NULL as increment otherwise does not work
    ,PRIMARY KEY(`id`)
    ,UNIQUE(urlHash)
    ) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- FILE VERSIONS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS fileVersions (
    id CHAR(32) CHARACTER SET ASCII NOT NULL -- the UUID
    ,fileMasterID INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,storageTypeID INT UNSIGNED NOT NULL
    ,actionAppliedID TINYINT UNSIGNED NOT NULL
    ,fileName VARCHAR(255) NULL
    ,mimeType VARCHAR(127) NULL
    ,pathOrBucket VARCHAR(2048) NULL
    ,versionTimestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,size INT UNSIGNED NULL
    ,hashMethod VARCHAR(6) NULL
    ,hashValue VARCHAR(512) NULL
    ,comment VARCHAR(256) NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`fileMasterID`)
    ,INDEX(`storageTypeID`)
    -- No index on fileName as too long for standard index
    ) ENGINE=InnoDB;


ALTER TABLE `fileVersions`
ADD CONSTRAINT `link from version to master`
FOREIGN KEY (`fileMasterID`)
REFERENCES `fileMaster`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

ALTER TABLE `fileVersions`
ADD CONSTRAINT `location-type`
FOREIGN KEY (`storageTypeID`)
REFERENCES `storageTypes`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

ALTER TABLE `fileVersions`
ADD CONSTRAINT `action-type`
FOREIGN KEY (`actionAppliedID`)
REFERENCES `actions`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

CREATE TRIGGER increment_version_count
AFTER INSERT ON fileVersions
FOR EACH ROW
UPDATE fileMaster
SET fileMaster.numVersions_t = (fileMaster.numVersions_t + 1)
WHERE fileMaster.id = NEW.fileMasterID;

DELIMITER //

CREATE TRIGGER decrement_version_count_or_delete_master
AFTER DELETE ON fileVersions
FOR EACH ROW
BEGIN

  UPDATE fileMaster
  SET fileMaster.numVersions_t = (fileMaster.numVersions_t - 1)
  WHERE fileMaster.id = OLD.fileMasterID;

  -- in case there is no version left: drop the file from the master table
  DElETE FROM fileMaster
  WHERE fileMaster.id = OLD.fileMasterID AND fileMaster.numVersions_t = 0;

END

//

DELIMITER ;


-- ----------------------------------------------------------
-- FILE CONTENT
--
-- Aistinct table instead of a filed in fileVersions in case
-- somebody uses "SELECT *". All version infos are
-- stored in fileVersions.
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS fileContent (
    versionID CHAR(32) CHARACTER SET ASCII NOT NULL
    ,pageContent MEDIUMTEXT NOT NULL
    ,PRIMARY KEY(`versionID`)
) ENGINE=InnoDB;

ALTER TABLE `fileContent`
ADD CONSTRAINT `link from fileContent to fileVersions`
FOREIGN KEY (`versionID`)
REFERENCES `fileVersions`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

CREATE TRIGGER removeContent
AFTER DELETE ON fileContent
FOR EACH ROW
DElETE FROM fileVersions
WHERE fileVersions.id = OLD.versionID;

-- ----------------------------------------------------------
-- STATISTICS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS statisticsHosts (
    fqdnHash CHAR(64) NOT NULL
    ,fqdn VARCHAR(255) NOT NULL
    ,firstSeen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
    ,lastSeen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP()
    ,successfulRequests INT UNSIGNED NOT NULL DEFAULT 0
    ,temporaryProblems INT UNSIGNED NOT NULL DEFAULT 0
    ,permamentErrors INT UNSIGNED NOT NULL DEFAULT 0
    ,hitRateLimit INT UNSIGNED NOT NULL DEFAULT 0
    ,PRIMARY KEY(`fqdnHash`)
    ,INDEX(`firstSeen`)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- LABELS
--
-- Labels can be attached to the master entry, or to a specific
-- version of a file.
--
-- As long the queue item has not been processed, there is no
-- entry in neither the filemaster nor the fileVersion table.
-- However we need to be able to assign the labels.
-- => The filemaster entry can be determined via the SHA-256 Hash
-- of the URL. Each version of a file is identified through an UUID
-- that is transfered from the queue.
-- => This makes it possible to store those labels while the
-- action is till on hold.
-- => This require clean-up if an action is removed from the
-- queue *without* having been executed, or if a version or
-- a filemaster entry is removed. Foreign keys cannot be used,
-- as the restraints would kick in while the item is only in
-- the queue. So triggers will be used instead.
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS labels (
    -- holds labels and their description
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,shortName VARCHAR(63) NOT NULL
    ,description TEXT DEFAULT NULL
    ,PRIMARY KEY(`id`)
    ,UNIQUE(`shortName`)
    ) ENGINE=InnoDB;
-- INSERT IGNORE increases the autoincrement value even if nothing
-- is inserted. The gaps in values can become large.
-- A workaround would require to change innodb settings,
-- or more complicated queries. See:
-- https://www.percona.com/blog/2011/11/29/avoiding-auto-increment-holes-on-innodb-with-insert-ignore/
-- However, as unsigned int has a range from 0 to 4,294,967,295
-- this should not be a problem.

CREATE TABLE IF NOT EXISTS labelToMaster (
    -- links fileMaster entries to labels
    id INT NOT NULL AUTO_INCREMENT
    ,labelID INT UNSIGNED NOT NULL
    ,urlHash CHAR(64) NOT NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`labelID`)
    ,INDEX(`urlHash`)
    ) ENGINE=InnoDB;

ALTER TABLE `labelToMaster`
ADD CONSTRAINT `l2m labelID to id in labels`
FOREIGN KEY (`labelID`)
REFERENCES `labels`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

CREATE TABLE IF NOT EXISTS labelToVersion (
    -- links versions of a file to labels
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,labelID INT UNSIGNED NOT NULL
    ,versionUUID CHAR(32) CHARACTER SET ASCII NOT NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`labelID`)
    ,INDEX(`VersionUUID`)
    ) ENGINE=InnoDB;

ALTER TABLE `labelToVersion`
ADD CONSTRAINT `l2v labelID to id in labels`
FOREIGN KEY (`labelID`)
REFERENCES `labels`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;


-- ----------------------------------------------------------
-- BLOCKLIST
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS blockList (
    fqdnHash CHAR(64) NOT NULL
    ,fqdn VARCHAR(255) NOT NULL
    ,comment Text NULL
    ,UNIQUE(fqdnHash)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- RATELIMIT
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS rateLimits (
    fqdnHash CHAR(64) NOT NULL
    ,fqdn VARCHAR(255) NOT NULL
    ,hitRateLimitAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
    ,noContactUntil TIMESTAMP NOT NULL
    ,PRIMARY KEY(fqdnHash)
    ,INDEX(noContactUntil)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- VIEWS
--
-- For easier access.
-- ----------------------------------------------------------

CREATE VIEW v_content AS
SELECT m.id AS masterFileID
    ,c.versionID
    ,v.versionTimestamp
    ,m.url
    ,m.urlHash
    ,v.mimeType
    ,c.pageContent
FROM fileContent AS c
LEFT JOIN fileVersions AS v
ON c.versionID = v.id
LEFT JOIN fileMaster as m
on c.versionID = m.id;

CREATE VIEW v_errors_in_queue AS
SELECT
q.id as queueID,
q.url as URL,
a.description as action,
q.causesError,
e.permanent,
e.short as error,
e.description as errorDescription
FROM queue as q
LEFT JOIN errorType as e ON q.causesError = e.id
LEFT JOIN actions as a ON q.action = a.id
WHERE q.causesError IS NOT NULL;


-- ----------------------------------------------------------
-- STORED PROCEDURES
-- ----------------------------------------------------------

DELIMITER $$

CREATE PROCEDURE delete_all_versions_SP (IN fileMasterID_p INT)
MODIFIES SQL DATA
BEGIN
-- TO DO: DOES NOT DELETE THE ACTUAL FILES
DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;
    -- remove all labels attached to versions:
    DELETE FROM labelToVersion WHERE versionID IN (
        SELECT id FROM fileVersions WHERE fileID = filemasterID_p
        );
    -- now as the CONSTRAINT does not interfere, remove all versions:
    DELETE FROM fileVersions WHERE fileMasterID = fileMasterID_p;

    -- remove all labels attached to the fileMaster:
    DELETE FROM labelToMaster WHERE masterID = fileMasterID_p;
    -- now as there are no versions and the label CONSTRAINT
    -- does not interfere, remove the entry in FileMaster:
    DELETE FROM fileMaster WHERE id = fileMasterID_p;
    COMMIT;

END $$



DELIMITER $$
CREATE PROCEDURE delete_from_queue_SP (IN queueID_p CHAR(32) CHARACTER SET ASCII)
MODIFIES SQL DATA
BEGIN
-- Not a transaction as designed to be called from within transactions.
-- Remove the queue-entry.

    DELETE FROM queue WHERE id = queueID_p;
    -- TO DO: extend this so it cleans up labels if the URL ends up being
    -- never processed and is not in the queue with another action!

END $$



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

    CALL delete_from_queue_SP (queueID_p);

    COMMIT;

END $$



-- --------------------------------------------------------
-- insert_content_SP:
-- Saves the content, transfers the labels from the queue,
-- and removes the queue item.
--
-- --------------------------------------------------------
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
    -- Unclear if a new entry was generated, or it failed with a warning
    -- as there already was one. So get the id via the Hash of the URL:
    SELECT id FROM fileMaster WHERE urlHash = url_hash_p INTO @fileMasterID;

    INSERT INTO fileVersions (id, fileMasterID, storageTypeID, mimeType, actionAppliedID)
    VALUES (queueID_p, @fileMasterID, 1, mimeType_p, actionAppliedID_p);

    INSERT INTO fileContent (versionID, pageContent)
    VALUES (queueID_p, text_p);

    CALL delete_from_queue_SP (queueID_p);

    COMMIT;
END $$
DELIMITER ;



-- --------------------------------------------------------
-- next_queue_object_SP:
-- A stored procedure to return the next object in the queue.
-- It does not return URLs which are temporarily blocked or
-- cause errors.
-- --------------------------------------------------------
DELIMITER $$
CREATE PROCEDURE next_queue_object_SP ()
NOT DETERMINISTIC
READS SQL DATA
BEGIN

    SELECT
    id
    ,action
    ,url
    ,urlHash
    ,prettifyHtml
    FROM queue
    WHERE (
        causesError IS NULL OR causesError IN (
            SELECT id FROM errorType WHERE permanent = 0)
            ) AND (
        fqdnHash NOT IN (SELECT fqdnHash FROM rateLimits WHERE noContactUntil > NOW())
        ) AND
    (delayUntil IS NULL OR delayUntil < NOW()) AND
    action IN (1, 2, 3, 4)
    ORDER BY addedToQueue ASC
    LIMIT 1;

END$$
DELIMITER ;


CREATE FUNCTION num_items_with_temporary_errors ()
-- Some errors are only temporary. So before the bot stops,
-- those have to be counted.
RETURNS INTEGER
RETURN(SELECT COUNT(*) FROM queue WHERE causesError IN (
    SELECT id FROM errorType WHERE permanent = 0)
    );

CREATE FUNCTION num_items_with_permanent_error ()
-- Count the number of errors the bot cannot resolve.
RETURNS INTEGER
RETURN(SELECT COUNT(*) FROM queue WHERE causesError IN (
    SELECT id FROM errorType WHERE permanent = 1)
    );


-- ----------------------------------------------------------
-- ----------------------------------------------------------
-- BELOW CHANGES FOR EXOSKELETON VERSION 1.1.0
-- ----------------------------------------------------------
-- ----------------------------------------------------------

-- Store some information about the installation
CREATE TABLE IF NOT EXISTS exoInfo (
    exoKey CHAR(32) CHARACTER SET ASCII NOT NULL
    ,exoValue VARCHAR(64) NOT NULL
    ,PRIMARY KEY(`exoKey`)
) ENGINE=InnoDB;

-- Store the version of the schema so exoskeleton can check if the schema 
-- fulfills the basic reuirements for the version:
INSERT INTO exoInfo VALUES ('schema', '1.1.0') 
ON DUPLICATE KEY UPDATE exoValue = '1.1.0';

-- ----------------------------------------------------------
-- ----------------------------------------------------------
-- BELOW CHANGES FOR EXOSKELETON VERSION 1.2.0
-- ----------------------------------------------------------
-- ----------------------------------------------------------

INSERT INTO exoInfo VALUES ('schema', '1.2.0') 
ON DUPLICATE KEY UPDATE exoValue = '1.2.0';


-- The view v_errors_in_queue had the database name exoskeleton hardcoded.
-- Drop it and recreate it.
DROP VIEW IF EXISTS v_errors_in_queue;

CREATE VIEW v_errors_in_queue AS
SELECT
q.id as queueID,
q.url as URL,
a.description as action,
q.causesError,
e.permanent,
e.short as error,
e.description as errorDescription
FROM queue as q
LEFT JOIN errorType as e ON q.causesError = e.id
LEFT JOIN actions as a ON q.action = a.id
WHERE q.causesError IS NOT NULL;

-- ----------------------------------------------------------
-- ----------------------------------------------------------
-- BELOW CHANGES FOR EXOSKELETON VERSION 1.4.0
-- ----------------------------------------------------------
-- ----------------------------------------------------------

INSERT INTO exoInfo VALUES ('schema', '1.4.0') 
ON DUPLICATE KEY UPDATE exoValue = '1.4.0';


CREATE FUNCTION num_tasks_with_active_rate_limit ()
-- Number of tasks in the queue that do not yield a permanent error,
-- but are currently affected by a rate limit.
RETURNS INTEGER
RETURN(
    SELECT COUNT(*) FROM queue
    WHERE causesError NOT IN (
        SELECT id FROM errorType
        WHERE permanent = 1)
    AND fqdnhash IN (
        SELECT fqdnhash FROM rateLimits
        WHERE noContactUntil > NOW())
    );


CREATE FUNCTION num_tasks_in_queue_without_error ()
-- Number of tasks in the queue, that are not marked as causing an error
-- (neither temporary nor permanent)
RETURNS INTEGER
RETURN(
    SELECT COUNT(*) FROM queue WHERE causesError IS NULL
);


-- A stored procedure to add an URL and the associated action to the task queue.
DELIMITER $$
CREATE PROCEDURE add_to_queue_SP (IN uuid_p CHAR(32) CHARACTER SET ASCII,
                                  IN action_p TINYINT UNSIGNED,
                                  IN url_p TEXT,
                                  IN fqdn_p VARCHAR(255),
                                  IN prettify_p TINYINT UNSIGNED)
MODIFIES SQL DATA
BEGIN
INSERT INTO queue (id, action, url, urlHash, fqdnHash, prettifyHtml)
VALUES (uuid_p, action_p, url_p, SHA2(url_p,256), SHA2(fqdn_p,256), prettify_p);
END $$
DELIMITER ;



-- block_fqdn_SP:
-- A stored procedure to put a specific fully qualified domain name FQDN
-- (not an URL) on the blocklist.
DELIMITER $$
CREATE PROCEDURE block_fqdn_SP (IN fqdn_p VARCHAR(255),
                                IN comment_p TEXT)
MODIFIES SQL DATA
BEGIN
INSERT INTO blockList (fqdn, fqdnHash, comment)
VALUES (fqdn_p, SHA2(fqdn_p,256), comment_p);
END $$
DELIMITER ;


-- Remove a previously blocked FQDN (not an URL) from the blocklist.
DELIMITER $$
CREATE PROCEDURE unblock_fqdn_SP (IN fqdn_p VARCHAR(255))
MODIFIES SQL DATA
BEGIN
DELETE FROM blockList WHERE fqdnHash = SHA2(fqdn_p,256);
END $$
DELIMITER ;

-- Empty the blocklist
DELIMITER $$
CREATE PROCEDURE truncate_blocklist_SP ()
MODIFIES SQL DATA
BEGIN
TRUNCATE TABLE blockList;
END $$
DELIMITER ;


CREATE FUNCTION fqdn_on_blocklist (fqdn_p VARCHAR(255))
-- Returns 0 if the FQDN is not on the blocklist, or an integer > 0 otherwise
RETURNS INTEGER
RETURN(
    SELECT COUNT(*) FROM blockList WHERE fqdnhash = SHA2(fqdn_p, 256)
    );

-- Update the central host based statistics by incrementing counters
-- held in the database.
DELIMITER $$
CREATE PROCEDURE update_host_stats_SP (IN fqdn_p VARCHAR(255),
                                       IN successfulRequests_p INT UNSIGNED,
                                       IN temporaryProblems_p INT UNSIGNED,
                                       IN permamentErrors_p INT UNSIGNED,
                                       IN hitRateLimit_p INT UNSIGNED)
MODIFIES SQL DATA
BEGIN
INSERT INTO statisticsHosts 
(fqdnHash, fqdn, successfulRequests, temporaryProblems, permamentErrors, hitRateLimit) 
VALUES (SHA2(fqdn_p,256), fqdn_p, successfulRequests_p, temporaryProblems_p, permamentErrors_p, hitRateLimit_p) 
ON DUPLICATE KEY UPDATE 
successfulRequests = successfulRequests + successfulRequests_p, 
temporaryProblems = temporaryProblems + temporaryProblems_p, 
permamentErrors = permamentErrors + permamentErrors_p, 
hitRateLimit = hitRateLimit + hitRateLimit_p;
END $$
DELIMITER ;


-- The bot hit a rate limit, so add wait time until it is contacted again.
DELIMITER $$
CREATE PROCEDURE add_rate_limit_SP (IN fqdn_p VARCHAR(255),
                                    IN wait_seconds_p INT UNSIGNED)
MODIFIES SQL DATA
BEGIN
INSERT INTO rateLimits (fqdnHash, fqdn, noContactUntil)
VALUES (SHA2(fqdn_p,256), fqdn_p, ADDTIME(NOW(), SEC_TO_TIME(wait_seconds_p)))  
ON DUPLICATE KEY UPDATE noContactUntil = ADDTIME(NOW(), SEC_TO_TIME(wait_seconds_p));
END $$
DELIMITER ;






-- Treat all queued tasks, that are marked to cause any type of error,
-- as if they were new tasks.
DELIMITER $$
CREATE PROCEDURE forget_all_errors_SP ()
MODIFIES SQL DATA
BEGIN
UPDATE queue SET causesError = NULL, numTries = 0, delayUntil = NULL;
END $$
DELIMITER ;


-- Remove the association between a label and the specific version of a file.
DELIMITER $$
CREATE PROCEDURE remove_labels_from_uuid_SP (
    IN label_id_p INT UNSIGNED,
    IN uuid_p CHAR(32) CHARACTER SET ASCII)
MODIFIES SQL DATA
BEGIN
DELETE FROM labelToVersion WHERE labelID = label_id_p and versionUUID = uuid_p;
END $$
DELIMITER ;


-- Create a new crawl job identified by its name and add an URL to start crawling
DELIMITER $$
CREATE PROCEDURE define_new_job_SP (IN job_name_p VARCHAR(127),
                                    IN start_url_p TEXT)
MODIFIES SQL DATA
BEGIN
INSERT INTO jobs (jobName, startUrl, startUrlHash)
VALUES (job_name_p, start_url_p, SHA2(start_url_p,256));
END $$
DELIMITER ;


-- Returns the status and the current URL for a job.
-- If no current URL is stored, return the start URL.
DELIMITER $$
CREATE PROCEDURE job_get_current_url_SP (IN job_name_p VARCHAR(127))
READS SQL DATA
BEGIN
SELECT finished, COALESCE(currentUrl, startUrl) AS url FROM jobs WHERE jobName = job_name_p;
END $$
DELIMITER ;

-- Return the names of all stored procedures in the database
DELIMITER $$
CREATE PROCEDURE db_check_all_procedures_SP (IN db_name_p VARCHAR(64))
READS SQL DATA
BEGIN
SELECT SPECIFIC_NAME 
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = db_name_p AND ROUTINE_TYPE = 'PROCEDURE';
END $$
DELIMITER ;

-- Return the names of all functions in the database
DELIMITER $$
CREATE PROCEDURE db_check_all_functions_SP (IN db_name_p VARCHAR(64))
READS SQL DATA
BEGIN
SELECT SPECIFIC_NAME 
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = db_name_p AND ROUTINE_TYPE = 'FUNCTION';
END $$
DELIMITER ;


CREATE FUNCTION exo_schema_version ()
-- Return the schema version of exoskeleton
RETURNS VARCHAR(64)
RETURN(
    SELECT exoValue FROM exoInfo WHERE exoKey = 'schema'
    );