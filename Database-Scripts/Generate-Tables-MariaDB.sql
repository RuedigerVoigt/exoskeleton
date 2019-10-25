-- ----------------------------------------------------------
-- EXOSKELETON TABLE STRUCTURE FOR MARIADB
-- © 2019 Rüdiger Voigt
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
-- ----------------------------------------------------------



-- ----------------------------------------------------------
-- QUEUE
-- 
-- Needed by the bot to know what to do next.
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS queue (
    id INT NOT NULL AUTO_INCREMENT
    ,action INT
    ,url VARCHAR(20148) NOT NULL
    ,addedToQueue TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,causesPermanentError INT NULL
    ,retries INT NULL
    ,delayUntil TIMESTAMP NULL
    ,PRIMARY KEY(`id`)
    ,INDEX(`action`)
    ,INDEX(`addedToQueue`)
    ,INDEX(`delayUntil`)
) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- ACTIONS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS actions (
    id INT NOT NULL
    ,description VARCHAR(256)
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;

INSERT INTO actions (id, description) VALUES 
(1, 'download file to disk'),
(2, 'save page code into database');

-- ----------------------------------------------------------
-- PERMANENT ERRORS
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS permanentErrors (
    id INT NOT NULL AUTO_INCREMENT
    ,short VARCHAR(15)
    ,description VARCHAR(255)
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;

INSERT INTO permanentErrors (short, description) VALUES 
('404', 'Server says: File not found'),
('410', 'Server says: Gone');

-- ----------------------------------------------------------
-- DOWNLOADED FILES
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS files (
    id INT NOT NULL AUTO_INCREMENT
    ,downloadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,url VARCHAR(20148) NOT NULL
    ,storageFolder VARCHAR(255) NULL
    ,fileName VARCHAR(255) NULL
    ,size INT NULL
    ,hashMethod VARCHAR(6) NULL
    ,hashValue VARCHAR(256) NULL
    ,PRIMARY KEY(`id`)
    ) ENGINE=InnoDB;

-- ----------------------------------------------------------
-- 
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS content (
    id INT NOT NULL AUTO_INCREMENT
    ,downloadDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ,url VARCHAR(20148) NOT NULL
    ,pageContent TEXT NOT NULL
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;


-- ----------------------------------------------------------
-- EVENT LOG
-- 
-- Log important events like start / end of a run, reaching
-- a milestone, ...
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS eventLog (
    id INT NOT NULL AUTO_INCREMENT
    ,logDate TIMESTAMP NOT NULL
    ,severity VARCHAR(12)
    ,message VARCHAR(1024) NOT NULL
    ,PRIMARY KEY(`id`)
) ENGINE=InnoDB;


-- ----------------------------------------------------------
-- REFERENTIAL INTEGRITY
-- ----------------------------------------------------------

ALTER TABLE `queue` 
ADD CONSTRAINT `errors` 
FOREIGN KEY (`causesPermanentError`) 
REFERENCES `permanentErrors`(`id`) 
ON DELETE RESTRICT  
ON UPDATE RESTRICT;

ALTER TABLE `queue` 
ADD CONSTRAINT `actions` 
FOREIGN KEY (`action`) 
REFERENCES `actions`(`id`) 
ON DELETE RESTRICT  
ON UPDATE RESTRICT;


