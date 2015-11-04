SET FOREIGN_KEY_CHECKS=0;

CREATE TABLE IF NOT EXISTS State (
  abbrev VARCHAR(2),  -- eg CA, AZ
  country VARCHAR(200), -- eg United States
  name VARCHAR(200), -- eg California, Arizona
  lastTouched DATETIME DEFAULT NOW(),

 PRIMARY KEY (abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

INSERT INTO State
(abbrev, country, name)
VALUES 
("CA", "United States", "California");


CREATE TABLE IF NOT EXISTS House (
  name VARCHAR(200), -- Name for the house. eg Assembly, Senate
  state VARCHAR(2),
  lastTouched DATETIME DEFAULT NOW(),

  PRIMARY KEY (name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
  )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

INSERT INTO House
(name, state)
VALUES
("Assembly", "CA"),
("Senate", "CA");

ALTER TABLE Person
  ADD lastTouched DATETIME DEFAULT NOW();

ALTER TABLE Legislator
  CHANGE room_number room_number VARCHAR(10),
  ADD state VARCHAR(2) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Legislator
  SET state = "CA";

ALTER TABLE servesOn
    DROP FOREIGN KEY servesOn_ibfk_1,
    CHANGE house house VARCHAR(200),
    ADD state VARCHAR(2),
    ADD lastTouched DATETIME DEFAULT NOW();

UPDATE servesOn
  SET state = "CA";

ALTER TABLE servesOn
    ADD FOREIGN KEY (state) REFERENCES State(abbrev);

ALTER TABLE servesOn
    ADD FOREIGN KEY (house, state) REFERENCES House(name, state),
    DROP PRIMARY KEY, 
    ADD PRIMARY KEY (pid, year, house, state, cid),
    DROP district;

ALTER TABLE Term 
    CHANGE house house VARCHAR(200),
    ADD state VARCHAR(2),
    ADD caucus VARCHAR(200),
    ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Term
  SET State = "CA";

ALTER TABLE Term 
    ADD FOREIGN KEY (house, state) REFERENCES House(name, state),
    ADD FOREIGN KEY (state) REFERENCES State(abbrev),
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (pid, year, house, state);


ALTER TABLE Committee
    ADD state VARCHAR(2),
    CHANGE house house VARCHAR(200),
    CHANGE type type VARCHAR(100),
    ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Committee
  SET state = "CA";

ALTER TABLE Committee
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD FOREIGN KEY (house, state) REFERENCES House(name, state);


ALTER TABLE servesOn
  ADD FOREIGN KEY (pid, year, house) REFERENCES Term(pid, year, house);


ALTER TABLE Bill
  CHANGE state billState ENUM('Chaptered', 'Introduced', 'Amended Assembly', 'Amended Senate', 'Enrolled',
              'Proposed', 'Amended', 'Vetoed') NOT NULL,
  CHANGE bid bid VARCHAR(23),
  ADD state VARCHAR(2),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Bill
  SET state = "CA";

ALTER TABLE Bill
  ADD FOREIGN KEY (state) REFERENCES State(abbrev);

  
ALTER TABLE Hearing
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Hearing
  SET state = "CA";


ALTER TABLE CommitteeHearings
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE Action
  CHANGE bid bid VARCHAR(23),
  ADD FOREIGN KEY (bid) REFERENCES Bill(bid),
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE Video
  ADD state VARCHAR(2),
  ADD CONSTRAINT Video_ibfk_2
    FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD source ENUM("YouTube", "Local", "Other"),
  CHANGE youtubeId fileId VARCHAR(20),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Video
  SET state = "CA";


ALTER TABLE Video_ttml
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE BillDiscussion
  CHANGE bid bid VARCHAR(23),
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE BillVoteSummary
  CHANGE bid bid VARCHAR(23);


ALTER TABLE BillVersion
  CHANGE state billState ENUM('Chaptered', 'Introduced', 'Amended Assembly', 'Amended Senate',
              'Enrolled', 'Proposed', 'Amended', 'Vetoed') NOT NULL,
  ADD state VARCHAR(2),
  CHANGE bid bid VARCHAR(23),
  CHANGE vid vid VARCHAR(33),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE BillVersion
  SET state = "CA";


ALTER TABLE authors
  CHANGE bid bid VARCHAR(23),
  CHANGE vid vid  VARCHAR(33),
  ADD FOREIGN KEY (bid, vid) REFERENCES BillVersion(bid, vid),
  ADD lastTouched DATETIME DEFAULT NOW();

  
ALTER TABLE CommitteeAuthors
  CHANGE bid bid VARCHAR(23),
  CHANGE vid vid  VARCHAR(33),
  ADD FOREIGN KEY (bid, vid) REFERENCES BillVersion(bid, vid),
  ADD state VARCHAR(2),
  ADD CONSTRAINT CommitteeAuthors_ibfk_4
    FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE CommitteeAuthors
  SET state = "CA";  


ALTER TABLE Utterance
  ADD state VARCHAR(2),
  ADD CONSTRAINT Utterance_ibfk_4
    FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Utterance
  SET state = "CA";  
  

ALTER VIEW currentUtterance 
  AS SELECT uid, vid, pid, did,
    time, endTime, text, type, alignment, state 
  FROM Utterance 
  WHERE current = TRUE AND finalized = TRUE ORDER BY time DESC;


ALTER TABLE BillVoteDetail
  ADD state VARCHAR(2),
  ADD CONSTRAINT BillVoteDetail_ibfk_2
    FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE BillVoteDetail
  SET state = "CA";  

  
ALTER TABLE Gift
  ADD state VARCHAR(2),
  ADD CONSTRAINT Gift_ibfk_3
    FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Gift
  SET state = "CA";  


ALTER TABLE District
  CHANGE house house VARCHAR(200),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE District
  SET state = "CA"; 


ALTER TABLE Contribution
  ADD state VARCHAR(2),
  ADD CONSTRAINT Contribution_ibfk_2
    FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Contribution
  SET state = "CA"; 


ALTER TABLE Organizations
  CHANGE state stateHeadquarted VARCHAR(2),
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE Lobbyist
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (pid, state),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Lobbyist
  SET state = "CA"; 


-- removing a repeat
DELETE FROM LobbyingFirm
WHERE filer_id = 'F01039';


CREATE TABLE LobbyingFirmState
AS
SELECT filer_id, rpt_date, ls_beg_yr, ls_end_yr, 
  filer_naml 
FROM LobbyingFirm;


ALTER TABLE LobbyistEmployment
  DROP FOREIGN KEY LobbyistEmployment_ibfk_1;


ALTER TABLE LobbyingFirm
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (filer_naml),
  DROP filer_id,
  DROP rpt_date,
  DROP ls_beg_yr,
  DROP ls_end_yr,
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE LobbyingFirmState
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD PRIMARY KEY (filer_id, state),
  ADD FOREIGN KEY (filer_naml) REFERENCES LobbyingFirm(filer_naml),
  ADD lastTouched DATETIME DEFAULT NOW();
  
UPDATE LobbyingFirmState
SET state = "CA"; 


ALTER TABLE LobbyistEmployment
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD FOREIGN KEY (pid) REFERENCES Person(pid),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE LobbyistEmployment
SET state = "CA"; 

ALTER TABLE LobbyistEmployment
  ADD FOREIGN KEY (sender_id, state) REFERENCES LobbyingFirmState(filer_id, state);


ALTER TABLE LobbyistEmployer
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD CONSTRAINT LobbyistEmployer_ibfk_2
    FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE LobbyistEmployer
SET state = "CA"; 

ALTER TABLE LobbyistEmployer
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (oid, state),
  ADD UNIQUE (filer_id, state);


ALTER TABLE LobbyistDirectEmployment
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev);


UPDATE LobbyistDirectEmployment
SET state = "CA"; 

ALTER TABLE LobbyistDirectEmployment
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (pid, sender_id, rpt_date, ls_end_yr, state),
  ADD FOREIGN KEY (sender_id, state) REFERENCES LobbyistEmployer(filer_id, state),
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE LobbyingContracts
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2), 
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();


UPDATE LobbyingContracts
SET state = "CA"; 

-- Totally broken
ALTER TABLE LobbyingContracts
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (filer_id, sender_id, rpt_date, state),
  ADD FOREIGN KEY (sender_id, state) REFERENCES LobbyistEmployer(filer_id, state),
  ADD FOREIGN KEY (filer_id, state) REFERENCES LobbyingFirmState(filer_id, state);


ALTER TABLE LobbyistRepresentation
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD CONSTRAINT LobbyistRepresentation_ibfk_4
     FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE LobbyistRepresentation
SET state = "CA"; 

SET FOREIGN_KEY_CHECKS=1;

ALTER TABLE GeneralPublic
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE GeneralPublic
SET state = "CA"; 


ALTER TABLE LegislativeStaff
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE LegislativeStaff
SET state = "CA"; 


ALTER TABLE LegislativeStaffRepresentation
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE LegislativeStaffRepresentation
SET state = "CA"; 


ALTER TABLE LegAnalystOffice
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE LegAnalystOffice
SET state = "CA"; 


ALTER TABLE LegAnalystOfficeRepresentation
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE LegAnalystOfficeRepresentation
SET state = "CA"; 


CREATE TABLE StateAgency
AS
SELECT DISTINCT employer AS name,
  "CA" AS state, NOW() AS lastTouched
FROM StateAgencyRep;


ALTER TABLE StateAgencyRep
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE StateAgencyRep
SET state = "CA"; 


ALTER TABLE StateAgencyRepRepresentation
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE StateAgencyRepRepresentation
SET state = "CA"; 

RENAME TABLE StateConstOffice TO StateConstOfficeRep;

ALTER TABLE StateConstOfficeRep
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE StateConstOfficeRep
SET state = "CA";


RENAME TABLE StateConstOfficeRepresentation TO 
  StateConstOfficeRepRepresentation;

ALTER TABLE StateConstOfficeRepRepresentation
  ENGINE = INNODB,
  CHARACTER SET utf8 COLLATE utf8_general_ci,
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE StateConstOfficeRepRepresentation
SET state = "CA";


ALTER TABLE Payors
  ADD addressState VARCHAR(2),
  ADD FOREIGN KEY (addressState) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE Behests
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE Behests
SET state = "CA";


ALTER TABLE BillAnalysis
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (State) REFERENCES State(abbrev),
  CHANGE bill_id bill_id VARCHAR(23),
  ADD lastTouched DATETIME DEFAULT NOW();

UPDATE BillAnalysis
SET state = "CA";

DROP VIEW BillAlignments;

CREATE VIEW BillAlignments
AS 
SELECT MAX(u.uid) AS uid, l.pid, u.alignment, u.did
FROM Lobbyist l
    JOIN currentUtterance u
    ON l.pid = u.pid
WHERE u.did IS NOT NULL
GROUP BY l.pid, u.alignment, u.did;

ALTER TABLE TT_Editor
  ADD state VARCHAR(2),
  ADD FOREIGN KEY (state) REFERENCES State(abbrev);

ALTER TABLE Motion
  ADD lastTouched DATETIME DEFAULT NOW();


ALTER TABLE BillVoteSummary
  ADD lastTouched DATETIME DEFAULT NOW();

SET FOREIGN_KEY_CHECKS = 0;

UPDATE Bill b1
SET b1.bid = CONCAT("CA_", b1.bid);


UPDATE Action
SET bid = CONCAT("CA_", bid);

UPDATE BillDiscussion
SET bid = CONCAT("CA_", bid);

UPDATE BillVoteSummary
SET bid = CONCAT("CA_", bid);

UPDATE BillVersion
SET bid = CONCAT("CA_", bid),
  vid = CONCAT("CA_", vid);

UPDATE authors
SET bid = CONCAT("CA_", bid),
  vid = CONCAT("CA_", vid);

UPDATE CommitteeAuthors
SET bid = CONCAT("CA_", bid),
  vid = CONCAT("CA_", vid);




-- DROP TABLE JobSnapShot; 
DROP TABLE attends;
DROP TABLE tag;
DROP TABLE Mention;
DROP TABLE user;
DROP TABLE BillDisRepresentation;
DROP TABLE TT_TaskCompletion;
