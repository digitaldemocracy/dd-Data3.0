-- Creates the organizations table
CREATE TABLE Organizations 
    AS (SELECT le_id AS oid, filer_naml AS name
        FROM LobbyistEmployer);

-- Adds city and state and type to organizations
ALTER TABLE Organizations
    ADD city VARCHAR(200),
    ADD state VARCHAR(2),
    ADD type INT DEFAULT 0;

-- Updates the primary key and makes the column
-- auto-increment
ALTER TABLE Organizations
    MODIFY oid INT AUTO_INCREMENT PRIMARY KEY;

-- Drops all foreign keys to le_id
ALTER TABLE BillDisRepresentation
    DROP FOREIGN KEY BillDisRepresentation_ibfk_3;

ALTER TABLE LobbyistRepresentation
    DROP FOREIGN KEY LobbyistRepresentation_ibfk_2;

-- Changes the name of le_id in LobbyistEmployer, and drops 
-- auto-increment
ALTER TABLE LobbyistEmployer
    CHANGE le_id oid INT;

-- Adds foreign keys back to LobbyistEmployer
ALTER TABLE BillDisRepresentation
    CHANGE le_id oid INT,
    ADD FOREIGN KEY (oid) REFERENCES LobbyistEmployer(oid);

ALTER TABLE LobbyistRepresentation
    CHANGE le_id oid INT,
    ADD FOREIGN KEY (oid) REFERENCES LobbyistEmployer(oid);

-- Adds fks to BillDiscussion for GeneralPublic and LobbyistRep
ALTER TABLE GeneralPublic
    ADD did INT,
    ADD FOREIGN KEY (did) REFERENCES BillDiscussion(did);

ALTER TABLE LobbyistRepresentation
    ADD did INT,
    ADD FOREIGN KEY (did) REFERENCES BillDiscussion(did);

-- Updates the primary keys of BillDis to include 
-- did as part of the pk
ALTER TABLE LobbyistRepresentation
    DROP PRIMARY KEY,
    ADD PRIMARY KEY(pid, oid, hid, did); 

-- Adds srtFlag to Video
ALTER TABLE Video
    ADD srtFlag TINYINT(1) DEFAULT 0;

-- Sets all srtFlag to -1 because reasons
UPDATE Video
    SET srtFlag = -1;

-- Gives GeneralPublic a fk to Organizations
ALTER TABLE GeneralPublic
    ADD oid INT,
    ADD FOREIGN KEY (oid) REFERENCES Organizations(oid);


-- Gives join_utrtag a fk to utterance like it should
ALTER TABLE join_utrtag
    ADD FOREIGN KEY (uid) REFERENCES Utterance(uid);

-- Drops auto-increment from Legislator
ALTER TABLE Legislator
    CHANGE pid pid INT;

-- Adds all affiliations to the Organizations table
INSERT INTO Organizations (name)
SELECT DISTINCT affiliation
FROM GeneralPublic
WHERE affiliation IS NOT NULL;

-- Updates Organizations so that oid has fk to proper affiliations
UPDATE GeneralPublic gp, Organizations o
SET gp.oid = o.oid
WHERE gp.affiliation = o.name;

-- drops the affiliation attribute from GeneralPublic 
ALTER TABLE GeneralPublic
DROP affiliation;

-- Creates new table to exist for behests
CREATE TABLE IF NOT EXISTS Payors(
    prid INT AUTO_INCREMENT,
    name VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),

    PRIMARY KEY(prid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- New table to account for all behests
CREATE TABLE IF NOT EXISTS Behests(
    official INT,
    datePaid DATE,
    payor INT,
    amount INT,
    payee INT,
    description TEXT,
    purpose VARCHAR(200), -- eg Charitable
    noticeReceived DATE, -- When filed, I think
    
    PRIMARY KEY(official, payor, payee, datePaid),
    FOREIGN KEY(official) REFERENCES Person(pid), 
    FOREIGN KEY(payor) REFERENCES Payors(prid),
    FOREIGN KEY(payee) REFERENCES Organizations(oid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- adds N/A as an enum option for alignment in Utterance
ALTER TABLE Utterance
    MODIFY alignment ENUM('For', 'Against', 
        'For_if_amend', 'Against_unless_amend', 
        'Neutral', 'Indeterminate', 'NA');

-- GeneralPublic needs the oid to default to 0
ALTER TABLE GeneralPublic
MODIFY oid INT DEFAULT 0;

-- removes filer_naml from LobbyistEmployer
ALTER TABLE LobbyistEmployer
DROP filer_naml;

ALTER TABLE Video_ttml
ADD source VARCHAR(4) DEFAULT 0;

-- LobbyistEmployer needs a foreign key to Organizations
ALTER TABLE LobbyistEmployer
ADD FOREIGN KEY (oid) REFERENCES Organizations(oid);
