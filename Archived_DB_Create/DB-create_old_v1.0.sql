-- file: DB-setup.sql
-- author: Daniel Mangin
-- date: 6/11/2015
-- End Usage: 6/18/2015
-- Description: Used to create all of the tables for Digital Democracy
-- note: this will only work on the currently used database
--
-- Change Log: Add DeprecatedPersons table
--
-- Description: We need to create new table called DeprecatedPerson. The table 
-- shall contain Pids of Person records that are no longer considered active.
--
-- Person deprecation happens when we discover duplicate records. When person 
-- merge happens, one of the records assumes the "powers" of the two duplicate 
-- records, the other gets deprecated. The Pid for this record will be inserted 
-- (by the Transcription tool) into the DeprecatedPerson record.

CREATE TABLE IF NOT EXISTS Person (
   pid    INTEGER AUTO_INCREMENT,
   last   VARCHAR(50) NOT NULL,
   first  VARCHAR(50) NOT NULL,
   image VARCHAR(256),
   -- description VARCHAR(1000), deprecated, moved to legislator profile

   PRIMARY KEY (pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- we can probably remove this table and roll into a "role" field in Person
-- alternatively, make it a (pid, jobhistory) and put term for the hearing in here
CREATE TABLE IF NOT EXISTS Legislator (
   pid         INTEGER AUTO_INCREMENT,
   description VARCHAR(1000),
   twitter_handle VARCHAR(100),
   capitol_phone  VARCHAR(30),
   website_url    VARCHAR(200),
   room_number    INTEGER,
   email_form_link VARCHAR(200),
   OfficialBio TEXT,
   
   PRIMARY KEY (pid),
   FOREIGN KEY (pid) REFERENCES Person(pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- we can probably remove this table and roll into a "role" field in Person
-- alternatively, we can make it (pid, jobhistory) and put client information here

-- CREATE TABLE IF NOT EXISTS Lobbyist (
--   pid    INTEGER,

--   PRIMARY KEY (pid),
--   FOREIGN KEY (pid) REFERENCES Person(pid)
-- )

-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;

-- only Legislators have Terms
CREATE TABLE IF NOT EXISTS Term (
   pid      INTEGER,
   year     YEAR,
   district INTEGER(3),
   house    ENUM('Assembly', 'Senate') NOT NULL,
   party    ENUM('Republican', 'Democrat', 'Other') NOT NULL,
   start    DATE,
   end      DATE,
   
   PRIMARY KEY (pid, year, district, house),
   FOREIGN KEY (pid) REFERENCES Legislator(pid) -- change to Person
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Committee (
   cid    INTEGER(3),
   house  ENUM('Assembly', 'Senate', 'Joint') NOT NULL,
   name   VARCHAR(200) NOT NULL,
   Type   ENUM('Standing','Select','Budget Subcommittee','Joint'),

   PRIMARY KEY (cid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS servesOn (
   pid      INTEGER,
   year     YEAR,
   district INTEGER(3),
   house    ENUM('Assembly', 'Senate') NOT NULL,
   cid      INTEGER(3),

   PRIMARY KEY (pid, year, district, house, cid),
   FOREIGN KEY (pid, year, district, house) REFERENCES Term(pid, year, district, house),
   FOREIGN KEY (cid) REFERENCES Committee(cid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Bill (
   bid     VARCHAR(20),
   type    VARCHAR(3) NOT NULL,
   number  INTEGER NOT NULL,
   state   ENUM('Chaptered', 'Introduced', 'Amended Assembly', 'Amended Senate', 'Enrolled',
      'Proposed', 'Amended', 'Vetoed') NOT NULL,
   status  VARCHAR(60),
   house   ENUM('Assembly', 'Senate', 'Secretary of State', 'Governor', 'Legislature'),
   session INTEGER(1),

   PRIMARY KEY (bid),
   INDEX name (type, number)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Hearing (
   hid    INTEGER AUTO_INCREMENT,
   date   DATE,

   PRIMARY KEY (hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS CommitteeHearings (
	cid INTEGER,
	hid INTEGER,

	PRIMARY KEY (cid, hid),
	FOREIGN KEY (cid) REFERENCES Committee(cid),
	FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS JobSnapshot (
   pid   INTEGER,
   hid   INTEGER,
   role  ENUM('Lobbyist', 'General_public', 'Legislative_staff_commitee', 'Legislative_staff_author', 'State_agency_rep', 'Unknown'),
   employer VARCHAR(50), -- employer: lobbyist: lobying firm, union, corporation. SAR: name of Agency/Department. GP: teacher/etc.
   client   VARCHAR(50), -- client: only for lobbyist

   PRIMARY KEY (pid, hid),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Action (
   bid    VARCHAR(20),
   date   DATE,
   text   TEXT,

   FOREIGN KEY (bid) REFERENCES Bill(bid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Video (
   vid INTEGER AUTO_INCREMENT,
   youtubeId VARCHAR(20),
   hid INTEGER,
   position INTEGER,
   startOffset INTEGER,
   duration INTEGER,

   PRIMARY KEY (vid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Video_ttml (
   vid INTEGER,
   ttml MEDIUMTEXT,

   FOREIGN KEY (vid) REFERENCES Video(vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- examine (without startTime in UNIQUE, duplicate)
CREATE TABLE IF NOT EXISTS BillDiscussion (
   did         INTEGER AUTO_INCREMENT,
   bid         VARCHAR(20),
   hid         INTEGER,
   startVideo  INTEGER,
   startTime   INTEGER,
   endVideo    INTEGER,
   endTime     INTEGER,
   numVideos   INTEGER(4),

   PRIMARY KEY (did),
   UNIQUE KEY (bid, startVideo, startTime),
   FOREIGN KEY (bid) REFERENCES Bill(bid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (startVideo) REFERENCES Video(vid),
   FOREIGN KEY (endVideo) REFERENCES Video(vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Motion (
   mid    INTEGER(20),
   date   DATETIME,
   text   TEXT,

   PRIMARY KEY (mid, date)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS votesOn (
   pid    INTEGER,
   mid    INTEGER(20),
   vote   ENUM('Yea', 'Nay', 'Abstain') NOT NULL,

   PRIMARY KEY (pid, mid),
   FOREIGN KEY (pid) REFERENCES Legislator(pid),
   FOREIGN KEY (mid) REFERENCES Motion(mid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS BillVersion (
   vid                 VARCHAR(30),
   bid                 VARCHAR(20),
   date                DATE,
   state               ENUM('Chaptered', 'Introduced', 'Amended Assembly', 'Amended Senate',
                            'Enrolled', 'Proposed', 'Amended', 'Vetoed') NOT NULL,
   subject             TEXT,
   appropriation       BOOLEAN,
   substantive_changes BOOLEAN,
   title               TEXT,
   digest              MEDIUMTEXT,
   text                MEDIUMTEXT,

   PRIMARY KEY (vid),
   FOREIGN KEY (bid) REFERENCES Bill(bid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS authors (
   pid          INTEGER,
   bid          VARCHAR(20),
   vid          VARCHAR(30),
   contribution ENUM('Lead Author', 'Principal Coauthor', 'Coauthor') DEFAULT 'Coauthor',

   PRIMARY KEY (pid, bid, vid),
   FOREIGN KEY (pid) REFERENCES Legislator(pid), -- change to Person
   FOREIGN KEY (bid, vid) REFERENCES BillVersion(bid, vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS attends (
   pid    INTEGER,
   hid    INTEGER,

   PRIMARY KEY (pid, hid),
   FOREIGN KEY (pid) REFERENCES Legislator(pid), -- Person
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- examine (without endTime in UNIQUE, duplicate)
CREATE TABLE IF NOT EXISTS Utterance (
   uid    INTEGER AUTO_INCREMENT,
   vid    INTEGER,
   pid    INTEGER,
   time   INTEGER,
   endTime INTEGER,
   text   TEXT,
   current BOOLEAN NOT NULL,
   finalized BOOLEAN NOT NULL,
   type   ENUM('Author', 'Testimony', 'Discussion'),
   alignment ENUM('For', 'Against', 'For_if_amend', 'Against_unless_amend', 'Neutral', 'Indeterminate'),
   dataFlag INTEGER DEFAULT 0,

   PRIMARY KEY (uid, current),
   UNIQUE KEY (uid, vid, pid, current, time),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (vid) REFERENCES Video(vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE OR REPLACE VIEW currentUtterance 
AS SELECT uid, vid, pid, time, endTime, text, type, alignment 
FROM Utterance 
WHERE current = TRUE AND finalized = TRUE ORDER BY time DESC;

-- tag is a keyword. For example, "education", "war on drugs"
-- can also include abbreviations for locations such as "Cal Poly" for "Cal Poly SLO"
CREATE TABLE IF NOT EXISTS tag (
   tid INTEGER AUTO_INCREMENT,
   tag VARCHAR(50),

   PRIMARY KEY (tid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- join table for Uterrance >>> Tag
CREATE TABLE IF NOT EXISTS join_utrtag (
   uid INTEGER,
   tid INTEGER,

   PRIMARY KEY (uid, tid),
   FOREIGN KEY (tid) REFERENCES tag(tid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- an utterance might contain an honorific or a pronoun where it is unclear who the actual person is
-- this is a "mention" and should be joined against when searching for a specific person 
CREATE TABLE IF NOT EXISTS Mention (
   uid INTEGER,
   pid INTEGER,

   PRIMARY KEY (uid, pid),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (uid) REFERENCES Utterance(uid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS BillVoteSummary (
	voteId 		INTEGER	AUTO_INCREMENT,
	bid		VARCHAR(20),
	mid		INTEGER(20),
	cid		INTEGER, 
	VoteDate	DATETIME,
	ayes		INTEGER,
	naes		INTEGER,
	abstain		INTEGER,
	result		VARCHAR(20),
	
	PRIMARY KEY(voteId),
	FOREIGN KEY (mid) REFERENCES Motion(mid),
	FOREIGN KEY (bid) REFERENCES Bill(bid),
	FOREIGN KEY (cid) REFERENCES Committee(cid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS BillVoteDetail (
	pid 	INTEGER,
	voteId 	INTEGER,
	result	VARCHAR(20),
	
	PRIMARY KEY(pid, voteId),
	FOREIGN KEY (voteId) REFERENCES BillVoteSummary(voteId)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Gift (
	RecordId INTEGER AUTO_INCREMENT,
	pid INTEGER,
	schedule ENUM('D', 'E'), -- D is a normal gift whereas E is a travel gift
	sourceName VARCHAR(50),
	activity VARCHAR(40),
	city VARCHAR(30),
	cityState VARCHAR(10),
	value DOUBLE,
	giftDate DATE,
	reimbursed TINYINT(1),
	giftIncomeFlag TINYINT(1) DEFAULT 0,
	speechFlag TINYINT(1) DEFAULT 0,
	description VARCHAR(80),
	
	PRIMARY KEY(RecordId),
	FOREIGN KEY (pid) REFERENCES Person(pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS District (
	state VARCHAR(2),
	house ENUM('lower', 'upper'),
	did INTEGER,
	note VARCHAR(40) DEFAULT '',
	year INTEGER,
	region TEXT,
	geoData MEDIUMTEXT,
	
	PRIMARY KEY(state, house, did, year)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Contribution (
	RecordId INTEGER AUTO_INCREMENT,
	pid INTEGER,
	law_eid INTEGER,
	d_id INTEGER,
	year INTEGER,
	house VARCHAR(10),
	contributor VARCHAR(50),
	amount DOUBLE,
	
	PRIMARY KEY(RecordId),
	FOREIGN KEY (pid) REFERENCES Person(pid)
)	
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- Transcription Tool Tables
CREATE TABLE IF NOT EXISTS TT_Editor (
   id INTEGER AUTO_INCREMENT , 
   username VARCHAR(50) NOT NULL , 
   password VARCHAR(255) NOT NULL , 
   created TIMESTAMP NOT NULL , 
   active BOOLEAN NOT NULL , 
   role VARCHAR(15) NOT NULL , 
   
   PRIMARY KEY (id),
   UNIQUE KEY (username)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS TT_Task (
   tid INTEGER AUTO_INCREMENT ,
   hid INTEGER ,	-- added
   did INTEGER , 
   editor_id INTEGER ,
   name VARCHAR(255) NOT NULL , 
   vid INTEGER , 
   startTime INTEGER NOT NULL , 
   endTime INTEGER NOT NULL , 
   created DATE,
   assigned DATE, 
   completed DATE,
   
   PRIMARY KEY (tid) ,
   FOREIGN KEY (did) REFERENCES BillDiscussion(did),
   FOREIGN KEY (editor_id) REFERENCES TT_Editor(id),
   FOREIGN KEY (vid) REFERENCES Video(vid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS TT_TaskCompletion (
   tcid INTEGER AUTO_INCREMENT , 
   tid INTEGER , 
   completion DATE , 
   
   PRIMARY KEY (tcid),
   FOREIGN KEY (tid) REFERENCES TT_Task(tid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS user (
   email VARCHAR(255) NOT NULL,
   name VARCHAR(255),
   password VARCHAR(255) NOT NULL,
   new_user INTEGER,

   PRIMARY KEY (email)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LobbyingFirm(
   filer_naml VARCHAR(200),
   filer_id VARCHAR(9)  PRIMARY KEY,  -- modified  (PK)
   rpt_date DATE,
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER     -- modified (INT)
);

--  ALTER TABLE !!!!!
CREATE TABLE IF NOT EXISTS Lobbyist(
   pid INTEGER,   -- added
   -- FILER_NAML VARCHAR(50),               modified, needs to be same as Person.last
   -- FILER_NAMF VARCHAR(50),               modified, needs to be same as Person.first  
   filer_id VARCHAR(9) UNIQUE,         -- modified   
   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid)
);

CREATE TABLE IF NOT EXISTS LobbyistEmployer(
   filer_naml VARCHAR(200),
   filer_id VARCHAR(9),  -- modified (PK)
   le_id INTEGER AUTO_INCREMENT,
   coalition TINYINT(1),
   
   PRIMARY KEY (le_id)
);

-- LOBBYIST_EMPLOYED_BY_LOBBYING_FIRM

CREATE TABLE IF NOT EXISTS LobbyistEmployment(
   pid INT  REFERENCES  Person(pid),                         -- modified (FK)
   sender_id VARCHAR(9) REFERENCES LobbyingFirm(filer_id), -- modified (FK)
   rpt_date DATE,
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,    -- modified (INT)

   PRIMARY KEY (pid, sender_id, rpt_date, ls_end_yr), -- modified (May 21)
   FOREIGN KEY (sender_id) REFERENCES LobbyingFirm(filer_id)
);

-- NEW TABLE: Lobbyist Employed Directly by Lobbyist Employers 
-- Structure same as LOBBYIST_EMPLOYED_BY_LOBBYING_FIRM, 
-- but the SENDER_ID is a Foreign Key onto LOBBYIST_EMPLOYER
--  LOBBYIST_EMPLOYED_BY_LOBBYIST_EMPLOYER

CREATE TABLE IF NOT EXISTS LobbyistDirectEmployment(
   pid INT  REFERENCES  Person(pid),                         
   sender_id VARCHAR(9) REFERENCES LobbyistEmployer(filer_id),
   rpt_date DATE,
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,     -- modified (INT)
   PRIMARY KEY (pid, sender_id, rpt_date, ls_end_yr) -- modified (May 21)
);

-- end new table


CREATE TABLE IF NOT EXISTS LobbyingContracts(
   filer_id VARCHAR(9) REFERENCES LobbyingFirm(filer_id),     -- modified (FK)
   sender_id VARCHAR(9) REFERENCES LobbyistEmployer(filer_id), -- modified (FK)
   rpt_date DATE,
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,     -- modified (INT)
   PRIMARY KEY (filer_id, sender_id, rpt_date) -- modified (May 21) 
);

CREATE TABLE IF NOT EXISTS LobbyistRepresentation(
   pid INTEGER REFERENCES Person(pid),                  -- modified
   le_id INTEGER, -- modified (renamed)
   hearing_date DATE,                                       -- modified (renamed)
   hid INTEGER,              -- added

   PRIMARY KEY(pid, le_id, hid),                 -- added
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (le_id) REFERENCES LobbyistEmployer(le_id)
);

CREATE TABLE IF NOT EXISTS GeneralPublic(
   pid INTEGER,   -- added
   affiliation VARCHAR(256),
   position VARCHAR(100),
   RecordId INTEGER AUTO_INCREMENT,  
   hid   INTEGER,						-- added

   PRIMARY KEY (RecordId),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
);

CREATE TABLE IF NOT EXISTS LegislativeStaff(
   pid INTEGER,   -- added
   flag TINYINT(1),  -- if flag is 0, there must be a legislator; if flag is 1, there must be a committee
   legislator INTEGER, -- this is the legislator 
   committee INTEGER,

   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (legislator) REFERENCES Person(pid),
   FOREIGN KEY (committee) REFERENCES Committee(cid),
   CHECK (Legislator IS NOT NULL AND flag = 0 OR committee IS NOT NULL AND flag = 1)
);

CREATE TABLE IF NOT EXISTS LegislativeStaffRepresentation(
   pid INTEGER,   -- added
   flag TINYINT(1),  -- if flag is 0, there must be a legislator; if flag is 1, there must be a committee
   legislator INTEGER, -- this is the legislator 
   committee INTEGER,
   hid   INTEGER,						-- added

   PRIMARY KEY (pid, hid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (legislator) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (committee) REFERENCES Committee(cid),
   CHECK (Legislator IS NOT NULL AND flag = 0 OR committee IS NOT NULL AND flag = 1)
);

CREATE TABLE IF NOT EXISTS LegAnalystOffice(
   pid INTEGER REFERENCES Person(pid), 

   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid)
);

CREATE TABLE IF NOT EXISTS LegAnalystOfficeRepresentation(
   pid INTEGER REFERENCES Person(pid),   -- added  
   hid   INTEGER,   					-- added

   PRIMARY KEY (pid, hid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
);

CREATE TABLE IF NOT EXISTS StateAgencyRep(
   pid INTEGER,   -- added
   employer VARCHAR(256),
   position VARCHAR(100),

   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid)
);

CREATE TABLE IF NOT EXISTS StateAgencyRepRepresentation(
   pid INTEGER,   -- added
   employer VARCHAR(256),
   position VARCHAR(100),   
   hid   INTEGER,						-- added

   PRIMARY KEY (pid, hid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
);

CREATE TABLE IF NOT EXISTS StateConstOffice(
   pid INTEGER,
   office VARCHAR(200),
   position VARCHAR(200),
   
   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid)
);

CREATE TABLE IF NOT EXISTS StateConstOfficeRepresentation(
   pid INTEGER,
   office VARCHAR(200),
   position VARCHAR(200),
   hid INTEGER,
   
   PRIMARY KEY (pid, hid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
);

CREATE TABLE IF NOT EXISTS BillDisRepresentation(
	did INTEGER,
	pid INTEGER,
	le_id INTEGER,
	hid INTEGER,
	
	PRIMARY KEY (did, pid, le_id, hid),
	FOREIGN KEY (did) REFERENCES BillDiscussion(did),
	FOREIGN KEY (pid) REFERENCES Person(pid),
	FOREIGN KEY (le_id) REFERENCES LobbyistEmployer(le_id),
	FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS CommitteeAuthors(
	cid INTEGER,
	bid VARCHAR(20),
	vid VARCHAR(30),
	
	PRIMARY KEY(cid, bid, vid),
	FOREIGN KEY (bid) REFERENCES Bill(bid),
	FOREIGN KEY (cid) REFERENCES Committee(cid),
	FOREIGN KEY (vid) REFERENCES BillVersion(vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;