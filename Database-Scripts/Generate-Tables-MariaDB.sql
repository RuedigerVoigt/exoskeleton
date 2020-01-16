-- ----------------------------------------------------------
-- EXOSKELETON TABLE STRUCTURE FOR MARIADB
-- for version 0.7.0 of exoskeleton
-- © 2019-2020 Rüdiger Voigt
-- APACHE-2 LICENSE
--
-- This file generates the table structure needed for the
-- exoskeleton framework in MariaDB.
-- For a documentation of the package, see:
-- https://github.com/RuedigerVoigt/exoskeleton
-- There you will also find scripts to create this table
-- structure in other dbms.
--
-- BEWARE: This should be run in an empty database as otherwise
-- this script might alter existing tables!
--
-- ----------------------------------------------------------

-- DO NOT FORGET TO CHANGE:
CREATE DATABASE `nameOfYourDatabase` DEFAULT CHARACTER SET utf8mb4;
USE `nameOfYourDatabase`;

-- ----------------------------------------------------------
-- QUEUE
--
-- Needed by the bot to know what to do next.
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS queue (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,action TINYINT UNSIGNED
    ,url TEXT NOT NULL
    ,urlHash CHAR(64) NOT NULL
    ,addedToQueue TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,causesError INT NULL
    ,retries INT NULL
    ,delayUntil TIMESTAMP NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`action`)
    ,UNIQUE(`urlHash`)
    ,INDEX(`addedToQueue`)
    ,INDEX(`delayUntil`)
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
(1, 'download file to disk'),
(2, 'save page code into database');


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
    ,short VARCHAR(15)
    ,description VARCHAR(255)
    ,permanent BOOLEAN
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;

INSERT INTO errorType (id, short, permanent, description) VALUES
(1, 'malformed url', 1, 'URL is malformed. Missing Schema (http:// or https://) ?'),
(402, '402', 1, 'Server replied: Payment Required'),
(403, '403', 1, 'Server replied: Forbidden'),
(404, '404', 1, 'Server replied: File not found'),
(405, '405', 1, 'Server replied: Method Not Allowed'),
(410, '410', 1, 'Server replied: Gone'),
(414, '414', 1, 'Server replied: URI too long'),
(429, '429', 0, 'Server replied: Too Many Requests'),
(451, '451', 1, 'Server replied: Unavailable For Legal Reasons'),
(500, '500', 0, 'Server replied: Internal Server Error'),
(503, '503', 0, 'Server replied: Service Unavailable');

ALTER TABLE `queue`
ADD CONSTRAINT `errors`
FOREIGN KEY (`causesError`)
REFERENCES `errorType`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

-- ----------------------------------------------------------
-- DOWNLOADED FILES
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS fileMaster (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,initialDownloadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,url TEXT NOT NULL
    ,urlHash CHAR(64) NOT NULL
    ,numVersions_t INT DEFAULT 0
    -- increment / decrement of version count via trigger in fileVersions
    -- Default has to be 0 not NULL as increment otherwise does not work
    ,PRIMARY KEY(`id`)
    ,UNIQUE(urlHash)
    ) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- FILE STORAGE
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS storageTypes (
    id INT UNSIGNED NOT NULL
    ,shortName VARCHAR(15)
    ,fullName VARCHAR(63)
    ,PRIMARY KEY(`id`)
    ) ENGINE=InnoDB;

INSERT INTO storageTypes (id, shortName, fullName) VALUES
(1, 'local', 'Local Database'),
(2, 'local', 'Local Filesystem'),
(3, 'AWS', 'Amazon Web Services'),
(4, 'GCP', 'Google Cloud Platform'),
(5, 'Azure', 'Microsoft Azure'),
(6, 'Alibaba', 'Alibaba');

CREATE TABLE IF NOT EXISTS fileVersions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,fileID INT UNSIGNED NOT NULL
    ,storageTypeID INT UNSIGNED NOT NULL
    ,fileName VARCHAR(255) NULL
    ,mimeType VARCHAR(127) NULL
    ,pathOrBucket VARCHAR(2048) NULL
    ,versionTimestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,size INT UNSIGNED NULL
    ,hashMethod VARCHAR(6) NULL
    ,hashValue VARCHAR(512) NULL
    ,comment VARCHAR(256) NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`fileID`)
    ,INDEX(`storageTypeID`)
    -- No index on fileName as too long for standard index
    ) ENGINE=InnoDB;


ALTER TABLE `fileVersions`
ADD CONSTRAINT `link from version to master`
FOREIGN KEY (`fileID`)
REFERENCES `fileMaster`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

ALTER TABLE `fileVersions`
ADD CONSTRAINT `location-type`
FOREIGN KEY (`storageTypeID`)
REFERENCES `storageTypes`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

CREATE TRIGGER increment_version_count
AFTER INSERT ON fileVersions
FOR EACH ROW
UPDATE fileMaster
SET fileMaster.numVersions_t = (fileMaster.numVersions_t + 1)
WHERE fileMaster.id = NEW.fileID;

DELIMITER //

CREATE TRIGGER decrement_version_count_or_delete_master
AFTER DELETE ON fileVersions
FOR EACH ROW
BEGIN

  UPDATE fileMaster
  SET fileMaster.numVersions_t = (fileMaster.numVersions_t - 1)
  WHERE fileMaster.id = OLD.fileID;

  -- in case there is no version left: drop the file from the master table
  DElETE FROM fileMaster
  WHERE fileMaster.id = OLD.fileID AND fileMaster.numVersions_t = 0;

END

//

DELIMITER ;


-- distinct table in Case somebody uses SELECT *
CREATE TABLE IF NOT EXISTS fileContent (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,versionID INT UNSIGNED NOT NULL
    ,pageContent MEDIUMTEXT NOT NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`versionID`)
) ENGINE=InnoDB;

ALTER TABLE `fileContent`
ADD CONSTRAINT `link from content to version in master`
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
-- EVENT LOG
--
-- Log important events like start / end of a run, reaching
-- a milestone, ...
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS eventLog (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,logDate TIMESTAMP NOT NULL
    ,severity VARCHAR(12)
    ,message VARCHAR(1024) NOT NULL
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- STATISTICS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS statisticsHosts (
    fqdnHash CHAR(32) NOT NULL
    ,fqdn VARCHAR(255) NOT NULL
    ,firstSeen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
    ,lastSeen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP()
    ,successful INT UNSIGNED NOT NULL DEFAULT 0
    ,problems INT UNSIGNED NOT NULL DEFAULT 0
    ,PRIMARY KEY(`fqdnHash`)
    ,INDEX(`firstSeen`)
) ENGINE=InnoDB;



-- ----------------------------------------------------------
-- SETTINGS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS settings (
    settingKey VARCHAR(31) NOT NULL
    -- not just "key" as this is reserved word
    ,settingValue VARCHAR(63) NOT NULL
    ,description VARCHAR(255) NULL
    ,PRIMARY KEY(`settingKey`)
    ) ENGINE=InnoDB;

INSERT IGNORE INTO settings
(settingKey, settingValue, description)
VALUES
('CONNECTION_TIMEOUT', '60','Seconds until a connection times out.'),
('FILE_HASH_METHOD', 'sha256','Method used for file hash'),
('MAIL_FINISH_MSG','True','True/False: send mail as soon the bot is done'),
('MAIL_START_MSG','True','True/False: send mail as soon the bot starts'),
('QUEUE_MAX_RETRY','3','BOT YET IMPLEMENTED: Int: Maximum number of retries if downloading a page/file failed.'),
('QUEUE_REVISIT','60','Seconds: Time to wait after the queue is empty to check for new elements.');

-- ----------------------------------------------------------
-- LABELS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS labels (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,shortName VARCHAR(63) NOT NULL
    ,description TEXT DEFAULT NULL
    ,PRIMARY KEY(`id`)
    ,UNIQUE(`shortName`)
    ) ENGINE=InnoDB;


CREATE TABLE IF NOT EXISTS labelToQueue (
    id INT NOT NULL AUTO_INCREMENT
    ,labelID INT UNSIGNED NOT NULL
    ,queueID INT UNSIGNED NOT NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`labelID`)
    ,INDEX(`queueID`)
    ) ENGINE=InnoDB;

ALTER TABLE `labelToQueue`
ADD CONSTRAINT `label to id in queue`
FOREIGN KEY (`labelID`)
REFERENCES `labels`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;



CREATE TABLE IF NOT EXISTS labelToMaster (
    id INT NOT NULL AUTO_INCREMENT
    ,labelID INT UNSIGNED NOT NULL
    ,masterID INT UNSIGNED NOT NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`labelID`)
    ,INDEX(`masterID`)
    ) ENGINE=InnoDB;

ALTER TABLE `labelToMaster`
ADD CONSTRAINT `label to id in labels`
FOREIGN KEY (`labelID`)
REFERENCES `labels`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;



CREATE TABLE IF NOT EXISTS labelToVersion (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT
    ,labelID INT UNSIGNED NOT NULL
    ,versionID INT UNSIGNED
    ,PRIMARY KEY(`id`)
    ,INDEX(`labelID`)
    ,INDEX(`versionID`)
    ) ENGINE=InnoDB;

ALTER TABLE `labelToVersion`
ADD CONSTRAINT `label for the version to id in labels`
FOREIGN KEY (`labelID`)
REFERENCES `labels`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

ALTER TABLE `labelToVersion`
ADD CONSTRAINT `label to document version id`
FOREIGN KEY (`versionID`)
REFERENCES `fileVersions`(`id`)
ON DELETE RESTRICT
ON UPDATE RESTRICT;

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
