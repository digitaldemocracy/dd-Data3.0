/*
file: DB-setup.sql
authors: Daniel Mangin
         Mandy Chan
         Andrew Voorhees

Start Date: 6/26/2015
End Usage: 

Description: Used to create all of the tables for Digital Democracy
note: this will only work on the currently used database

Change Log: DDDB2015July Initial Schema
   Added:
      - Behests Table
      - Payors  Table
      - Organizations Table
      - DeprecatedOrganization Table

   Modified:
      - LobbyistEmployer
         - filer_naml & le_id -> Organizations(oid)
      - LobbyistRepresentation
         - le_id -> Organizations(oid)
         - Added column: did
      - GeneralPublic
         - affiliation -> Organizations(oid)
         - Added column: did

Explanation:
      - Added Behest data into DDDB
      - Added Organization table and modified several tables to 
         point to that table
      - Modified Lobbyist and General Public Representations to 
         be by bill discussion and not by hearing
*/

/*****************************************************************************/
/*
  Represents a state. e.g. California, Arizona
*/
CREATE TABLE IF NOT EXISTS State (
  abbrev  VARCHAR(2),  -- eg CA, AZ
  country  VARCHAR(200), -- eg United States
  name   VARCHAR(200), -- eg Caliornia, Arizona
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(), 
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (abbrev)
  )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* 
  A house in a legislature. Necessary because different states can have 
  different names for their houses
*/
CREATE TABLE IF NOT EXISTS House (
  name  VARCHAR(100), -- Name for the house. eg Assembly, Senate
  state VARCHAR(2),
  type VARCHAR(100),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
  )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

  
/* Entity::Person

   Describes any person. Can be a Legislator, GeneralPublic, Lobbyist, etc.
*/
CREATE TABLE IF NOT EXISTS Person (
   pid    INTEGER AUTO_INCREMENT,   -- Person id
   last   VARCHAR(50) NOT NULL,     -- last name
   middle VARCHAR(50),              -- middle name
   first  VARCHAR(50) NOT NULL,     -- first name
   image VARCHAR(256),              -- path to image (if exists)
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(), 

   PRIMARY KEY (pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/* Entity::Organizations

   Organizations are companies or organizations.
*/
CREATE TABLE IF NOT EXISTS Organizations (
    oid INTEGER AUTO_INCREMENT,  -- Organization id
    name VARCHAR(200),           -- name
    type INTEGER DEFAULT 0,      -- type (not fleshed out yet)
    city VARCHAR(200),           -- city
    stateHeadquartered VARCHAR(2), -- U.S. state, where it's based
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

    PRIMARY KEY (oid)
    )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/* Entity::Legislator

   A legislator has a description and bio and several contact information.
*/
CREATE TABLE IF NOT EXISTS Legislator (
   pid         INTEGER,          -- Person id (ref. Person.pid)
   description VARCHAR(1000),    -- description
   twitter_handle VARCHAR(100),  -- twitter handle (ex: @example)
   capitol_phone  VARCHAR(30),   -- phone number (format: (xxx) xxx-xxxx)
   website_url    VARCHAR(200),  -- url
   room_number    VARCHAR(10),       -- room number
   email_form_link VARCHAR(200), -- email link
   OfficialBio TEXT,             -- bio
   state    VARCHAR(2), -- state where term was served
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,
   
   PRIMARY KEY (pid, state),
   FOREIGN KEY (pid) REFERENCES Person(pid), 
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Weak Entity::Term

   Legislators have Terms. For each term a legislator serves, keep track of 
   what district, house, and party they are associated with because legislators 
   can change those every term.
*/
CREATE TABLE IF NOT EXISTS Term (
   pid      INTEGER,    -- Person id (ref. Person.pised)
   year     YEAR,       -- year served
   district INTEGER(3), -- district legislator served in
   house    VARCHAR(100), -- house they serve in,
   party    ENUM('Republican', 'Democrat', 'Other'),
   start    DATE,       -- start date of term
   end      DATE,       -- end date of term
   state    VARCHAR(2), -- state where term was served
   -- caucus   VARCHAR(200), -- group that generally votes together. Not 
                             -- currently in use
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, year, house, state),
   FOREIGN KEY (pid) REFERENCES Legislator(pid), -- change to 
   FOREIGN KEY (house, state) REFERENCES House(name, state), 
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::Committee

   When a bill is introduced in either the Senate or the House, it is sent to a
   standing committee for study and to receive public comment. The committee 
   makes an initial determination if the proposal should go forward in the 
   legislature. If it votes to do so, the committee can suggest amendments to 
   the bill, approve it for further action by the full Senate or House, or 
   disapprove it.
*/
CREATE TABLE IF NOT EXISTS Committee (
   cid    INTEGER(3),               -- Committee id
   house  VARCHAR(200) NOT NULL,
   name   VARCHAR(200) NOT NULL,    -- committee name
   short_name   VARCHAR(200) NOT NULL,    -- committee name
   type   VARCHAR(100),
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (cid),
   FOREIGN KEY (state) REFERENCES State(abbrev), 
   FOREIGN KEY (house, state) REFERENCES House(name, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Relationship::servesOn(many-to-many) << [Committee, Term]

   A legislator (in a specific term) can serve on one or more committees.
*/
CREATE TABLE IF NOT EXISTS servesOn (
   pid      INTEGER,                               -- Person id (ref. Person.pid)
   year     YEAR,                                  -- year served
   house    VARCHAR(100),
   cid      INTEGER(3),                            -- Committee id (ref. Committee.cid)
   position ENUM('Chair', 'Vice-Chair', 'Co-Chair', 'Member'),
   state    VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, year, house, state, cid),
   FOREIGN KEY (cid) REFERENCES Committee(cid), 
   FOREIGN KEY (house, state) REFERENCES House(name, state),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::Bill

   A legislator (Senator/Assembly Member) or Committee can author a bill. It 
   goes through the legislative process and changes states and versions multiple 
   times. The house is where the bill was introduced in. The session indicates
   what legislative session was occurring when the bill was introduced.
*/
CREATE TABLE IF NOT EXISTS Bill (
   bid     VARCHAR(23),          -- Bill id (concat of state+years+session+type+number)
   type    VARCHAR(5) NOT NULL,  -- bill type abbreviation
   number  INTEGER NOT NULL,     -- bill number
   billState   ENUM('Chaptered', 'Introduced', 'Amended Assembly', 'Amended Senate', 'Enrolled',
      'Proposed', 'Amended', 'Vetoed') NOT NULL,
   status  VARCHAR(60),          -- current bill status
   house   VARCHAR(100),
   session INTEGER(1),           -- 0: Normal session, 1: Special session
   sessionYear YEAR(4), 
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (bid),
   FOREIGN KEY (state) REFERENCES State(abbrev),
   INDEX name (type, number)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::Hearing

   There are many hearings per day. A bill is presented during a hearing and 
   testimonies may be heard in support or opposition to the bill. During the 
   hearing, a committee will vote on the bill.
*/
CREATE TABLE IF NOT EXISTS Hearing (
   hid    INTEGER AUTO_INCREMENT,      -- Hearing id
   date   DATE,                        -- date of hearing
   type ENUM('Regular', 'Budget', 'Informational', 'Summary') DEFAULT 'Regular',
   state  VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),


   PRIMARY KEY (hid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Relationship::CommitteeHearings(many-to-many) << [Committee, Hearing]

   After the bill is introduced, a bill is assigned a policy committee according
   to subject area. During the committee hearing, the author presents the bill to 
   the committee. Testimonies may be heard in support or opposition to the bill. 
   The committee then votes on whether to pass the bill out of the committee, or 
   that it be passed as amended.
*/
CREATE TABLE IF NOT EXISTS CommitteeHearings (
    cid INTEGER,  -- Committee id (ref. Committee.cid)
    hid INTEGER,  -- Hearing id (ref. Hearing.hid)
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT,

    PRIMARY KEY (cid, hid),
    FOREIGN KEY (cid) REFERENCES Committee(cid),
    FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


-- Used to hold the many-to-many relationship btw Hearings and Bills. 
CREATE TABLE IF NOT EXISTS HearingAgenda (
    hid INTEGER,  -- Hearing id (ref. Hearing.hid)
    bid VARCHAR(23),
    date_created DATE, -- The date the agenda info was posted 
    current_flag TINYINT(1), -- Whether this is the most recent agenda
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT,

    PRIMARY KEY (hid, bid, date_created),
    FOREIGN KEY (bid) REFERENCES Bill(bid),
    FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE IF NOT EXISTS Action (
   bid    VARCHAR(23),
   date   DATE,
   text   TEXT,
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   FOREIGN KEY (bid) REFERENCES Bill(bid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Video (
   vid INTEGER AUTO_INCREMENT,
   fileId VARCHAR(20), -- formerly youtubeId. Our name for file 
   hid INTEGER,
   position INTEGER,
   startOffset INTEGER,
   duration INTEGER,
   srtFlag TINYINT(1) DEFAULT 0,
   state VARCHAR(2),
   source ENUM("YouTube", "Local", "Other"),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (vid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Video_ttml (
   vid INTEGER,
   version INTEGER DEFAULT 0,
   ttml MEDIUMTEXT,
   source VARCHAR(4) DEFAULT 0,
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   FOREIGN KEY (vid) REFERENCES Video(vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- examine (without startTime in UNIQUE, duplicate)
CREATE TABLE IF NOT EXISTS BillDiscussion (
   did         INTEGER AUTO_INCREMENT,
   bid         VARCHAR(23),
   hid         INTEGER,
   startVideo  INTEGER,
   startTime   INTEGER,
   endVideo    INTEGER,
   endTime     INTEGER,
   numVideos   INTEGER(4),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

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
   doPass TINYINT(1),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (mid, date)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS BillVoteSummary (
    voteId      INTEGER AUTO_INCREMENT,
    bid     VARCHAR(23),
    mid     INTEGER(20),
    cid     INTEGER, 
    VoteDate    DATETIME,
    ayes        INTEGER,
    naes        INTEGER,
    abstain     INTEGER,
    result      VARCHAR(20),
    VoteDateSeq INT,
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT,
    
    PRIMARY KEY(voteId),
    FOREIGN KEY (mid) REFERENCES Motion(mid),
    FOREIGN KEY (bid) REFERENCES Bill(bid),
    FOREIGN KEY (cid) REFERENCES Committee(cid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE IF NOT EXISTS BillVersion (
   vid                 VARCHAR(33),
   bid                 VARCHAR(23),
   date                DATE,
   billState               ENUM('Chaptered', 'Introduced', 'Amended Assembly', 'Amended Senate',
                            'Enrolled', 'Proposed', 'Amended', 'Vetoed') NOT NULL,
   subject             TEXT,
   appropriation       BOOLEAN,
   substantive_changes BOOLEAN,
   title               TEXT,
   digest              MEDIUMTEXT,
   text                MEDIUMTEXT,
   state               VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (vid),
   FOREIGN KEY (bid) REFERENCES Bill(bid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- Note that for now contribution is either always lead author 
-- or blank
CREATE TABLE IF NOT EXISTS authors (
   pid          INTEGER,
   bid          VARCHAR(23),
   vid          VARCHAR(33),
   contribution ENUM('Lead Author', 'Principal Coauthor', 'Coauthor') DEFAULT 'Coauthor',
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, bid, vid),
   FOREIGN KEY (pid) REFERENCES Legislator(pid), -- change to Person
   FOREIGN KEY (bid, vid) REFERENCES BillVersion(bid, vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS BillSponsorRolls (
    roll VARCHAR(100),
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT,

    PRIMARY KEY (roll)
    )
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Table basically just the same info as authors, but it clarifies their 
role. We have a second table as not to confuse the druple scripts that 
pull author names. Ideally we role this into authors soon */
CREATE TABLE IF NOT EXISTS BillSponsors (
   pid          INTEGER,
   bid          VARCHAR(23),
   vid          VARCHAR(33),
   contribution VARCHAR(100),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, bid, vid, contribution),
   FOREIGN KEY (pid) REFERENCES Legislator(pid), 
   FOREIGN KEY (bid, vid) REFERENCES BillVersion(bid, vid),
   FOREIGN KEY (contribution) REFERENCES BillSponsorRolls(roll)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE IF NOT EXISTS CommitteeAuthors(
    cid INTEGER,
    bid VARCHAR(23),
    vid VARCHAR(33),
    state VARCHAR(2),
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT,
    
    PRIMARY KEY(cid, bid, vid),
    FOREIGN KEY (bid) REFERENCES Bill(bid),
    FOREIGN KEY (cid) REFERENCES Committee(cid),
    FOREIGN KEY (vid) REFERENCES BillVersion(vid),
    FOREIGN KEY (state) REFERENCES State(abbrev)
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
   alignment ENUM('For', 'Against', 'For_if_amend', 'Against_unless_amend', 'Neutral', 'Indeterminate', 'NA'),
   dataFlag INTEGER DEFAULT 0,
   diarizationTag VARCHAR(5) DEFAULT '',
   did INT,
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (uid),
   UNIQUE KEY (uid, vid, pid, current, time),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (vid) REFERENCES Video(vid),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE OR REPLACE VIEW currentUtterance 
AS SELECT uid, vid, pid, time, endTime, text, type, alignment, state, did, 
  lastTouched
FROM Utterance 
WHERE current = TRUE AND finalized = TRUE ORDER BY time DESC;

CREATE TABLE IF NOT EXISTS BillVoteDetail (
    pid     INTEGER,
    voteId INTEGER,
    result  VARCHAR(20),
    state   VARCHAR(2),
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT,
    
    PRIMARY KEY(pid, voteId),
    FOREIGN KEY (state) REFERENCES State(abbrev),
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
    oid INT, -- Just matched from sourceName
    state VARCHAR(2),
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    
    PRIMARY KEY(RecordId),
    FOREIGN KEY (oid) REFERENCES Organizations(oid),
    FOREIGN KEY (pid) REFERENCES Person(pid),
    FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS District (
    state VARCHAR(2),
    house VARCHAR(100),
    did INTEGER,
    note VARCHAR(40) DEFAULT '',
    year INTEGER,
    region TEXT,
    geoData MEDIUMTEXT,
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT,
    
    PRIMARY KEY(state, house, did, year),
    FOREIGN KEY (state) REFERENCES State(abbrev),
    FOREIGN KEY (house, state) REFERENCES House(name, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Contribution (
    id VARCHAR(20),
    pid INTEGER,
    year INTEGER,
    date DATETIME,
    house VARCHAR(10),
    donorName VARCHAR(255),
    donorOrg VARCHAR(255),
    amount DOUBLE,
    oid INT, -- just matched from donorOrg
    state VARCHAR(2),
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    
    PRIMARY KEY(id),
    FOREIGN KEY (oid) REFERENCES Organizations(oid),
    FOREIGN KEY (pid) REFERENCES Person(pid),
    FOREIGN KEY (state) REFERENCES State(abbrev)
)   
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE IF NOT EXISTS Lobbyist(
   pid INTEGER,   -- added
   -- FILER_NAML VARCHAR(50),               modified, needs to be same as Person.last
   -- FILER_NAMF VARCHAR(50),               modified, needs to be same as Person.first  
   filer_id VARCHAR(9) UNIQUE,         -- modified, start with state prefix   
   state VARCHAR(2), 
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, state),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LobbyingFirm(
   filer_naml VARCHAR(200),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (filer_naml)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LobbyingFirmState (
   filer_id VARCHAR(50),  -- modified, given by state  
   rpt_date DATE,
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,     -- modified (INT)
   filer_naml VARCHAR(200),
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (filer_id, state),
   FOREIGN KEY (state) REFERENCES State(abbrev),
   FOREIGN KEY (filer_naml) REFERENCES LobbyingFirm(filer_naml)
    )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LobbyistEmployer(
   filer_id VARCHAR(50),  -- modified (PK)
   oid INTEGER,
   coalition TINYINT(1),
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,
   
   PRIMARY KEY (oid),
   UNIQUE (filer_id, state),
   FOREIGN KEY (oid) REFERENCES Organizations(oid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- LOBBYIST_EMPLOYED_BY_LOBBYING_FIRM

CREATE TABLE IF NOT EXISTS LobbyistEmployment(
   pid INT,                         -- modified (FK)
   sender_id VARCHAR(50), 
   rpt_date DATE,
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,    -- modified (INT)
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, sender_id, rpt_date, ls_end_yr), -- modified (May 21)
   FOREIGN KEY (sender_id, state) REFERENCES LobbyingFirmState(filer_id, state),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


-- NEW TABLE: Lobbyist Employed Directly by Lobbyist Employers 
-- Structure same as LOBBYIST_EMPLOYED_BY_LOBBYING_FIRM, 
-- but the SENDER_ID is a Foreign Key onto LOBBYIST_EMPLOYER
--  LOBBYIST_EMPLOYED_BY_LOBBYIST_EMPLOYER

CREATE TABLE IF NOT EXISTS LobbyistDirectEmployment(
   pid INT,
   sender_id VARCHAR(50), -- no longer used
   rpt_date DATE,
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,     -- modified (INT)
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, sender_id, rpt_date, ls_end_yr, state), -- modified (May 21)
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (sender_id, state) REFERENCES LobbyistEmployer(filer_id, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


-- end new table


CREATE TABLE IF NOT EXISTS LobbyingContracts(
   filer_id VARCHAR(50),
   sender_id VARCHAR(50), -- modified (FK)
   rpt_date DATE,
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,     -- modified (INT)
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (filer_id, sender_id, rpt_date, state), -- modified (May 21) 
   FOREIGN KEY (sender_id, state) REFERENCES LobbyistEmployer(filer_id, state),
   FOREIGN KEY (filer_id, state) REFERENCES LobbyingFirmState(filer_id, state),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE IF NOT EXISTS LobbyistRepresentation(
   pid INTEGER REFERENCES Person(pid),                  -- modified
   oid INTEGER, -- modified (renamed)
   hearing_date DATE,                                       -- modified (renamed)
   hid INTEGER,              -- added
   did INTEGER,
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY(pid, oid, hid, did),                 -- added
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (oid) REFERENCES LobbyistEmployer(oid),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS GeneralPublic(
   pid INTEGER,   -- added
   position VARCHAR(100),
   RecordId INTEGER AUTO_INCREMENT,  
   hid   INTEGER,                       -- added
   did INTEGER,
   oid INTEGER, 
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (RecordId),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did),
   FOREIGN KEY (oid) REFERENCES Organizations(oid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LegislativeStaff(
   pid INTEGER,   -- added
   flag TINYINT(1),  -- if flag is 0, there must be a legislator; if flag is 1, there must be a committee
   legislator INTEGER, -- this is the legislator 
   committee INTEGER,
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (legislator) REFERENCES Person(pid),
   FOREIGN KEY (committee) REFERENCES Committee(cid),
   FOREIGN KEY (state) REFERENCES State(abbrev),
   CHECK (Legislator IS NOT NULL AND flag = 0 OR committee IS NOT NULL AND flag = 1)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LegislativeStaffRepresentation(
   pid INTEGER,   -- added
   flag TINYINT(1),  -- if flag is 0, there must be a legislator; if flag is 1, there must be a committee
   legislator INTEGER, -- this is the legislator 
   committee INTEGER,
   hid   INTEGER,                       -- added
   state VARCHAR(2),
   did INT,
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, hid, did),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (legislator) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (committee) REFERENCES Committee(cid),
   FOREIGN KEY (state) REFERENCES State(abbrev),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did),
   CHECK (Legislator IS NOT NULL AND flag = 0 OR committee IS NOT NULL AND flag = 1)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LegAnalystOffice(
   pid INTEGER REFERENCES Person(pid), 
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LegAnalystOfficeRepresentation(
   pid INTEGER REFERENCES Person(pid),   -- added  
   hid   INTEGER,                       -- added
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, hid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS StateAgency (
  name VARCHAR(200),
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
  )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS StateAgencyRep(
   pid INTEGER,   -- added
   employer VARCHAR(200),
   position VARCHAR(100),
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (state) REFERENCES State(abbrev),
   FOREIGN KEY (employer, state) REFERENCES StateAgency(name, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS StateAgencyRepRepresentation(
   pid INTEGER,   -- added
   employer VARCHAR(200),
   position VARCHAR(100),   
   hid   INTEGER,                       -- added
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, hid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (state) REFERENCES State(abbrev),
   FOREIGN KEY (employer, state) REFERENCES StateAgency(name, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE IF NOT EXISTS StateConstOffice (
  name VARCHAR(200),
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
  )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS StateConstOfficeRep(
   pid INTEGER,
   office VARCHAR(200),
   position VARCHAR(200),
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,
   
   PRIMARY KEY (pid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (state) REFERENCES State(abbrev),
   FOREIGN KEY (office, state) REFERENCES StateConstOffice(name, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS StateConstOfficeRepRepresentation(
   pid INTEGER,
   office VARCHAR(200),
   position VARCHAR(200),
   hid INTEGER,
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,
   
   PRIMARY KEY (pid, hid),                    -- added
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (state) REFERENCES State(abbrev),
   FOREIGN KEY (office, state) REFERENCES StateConstOffice(name, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::Payors

   Payors are persons or organizations that pay for another organization 
   on the behest of a legislator.
*/
CREATE TABLE IF NOT EXISTS Payors(
    prid INT AUTO_INCREMENT,  -- Payor id
    name VARCHAR(200),        -- name
    city VARCHAR(50),         -- city
    addressState VARCHAR(2),         -- U.S. state
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

    PRIMARY KEY(prid),
    FOREIGN KEY (addressState) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::Behests

   Behests are when a legislator asks someone (can be an Organization or a 
   Person) to pay for another organization. Essentially, the legislator gets 
   good publicity for helping the payee. Later on, the payor can influence 
   the legislator on certain bills because they helped out before.
*/
CREATE TABLE IF NOT EXISTS Behests(
    official INT,          -- legislator (ref. Legislator.pid)
    datePaid DATE,         -- date the payor paid
    payor INT,             -- organization/person that paid (ref. Payors.pid)
    amount INT,            -- amount given to payee in USD
    payee INT,             -- organization that was paid (ref. Organizations.oid)
    description TEXT,      -- description of the exchange
    purpose VARCHAR(200),  -- purpose of behest (ex. Charitable)
    noticeReceieved DATE,  -- when the behest was filed
    state VARCHAR(2),
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT,

    PRIMARY KEY(official, payor, payee, datePaid),
    FOREIGN KEY(official) REFERENCES Person(pid), 
    FOREIGN KEY(payor) REFERENCES Payors(prid),
    FOREIGN KEY(payee) REFERENCES Organizations(oid),
    FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- The next two tables are added for Toshi
CREATE TABLE IF NOT EXISTS BillTypes (
  Type VARCHAR(10),
  Label VARCHAR(10),
  House VARCHAR(100),
  State VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  dr_id INTEGER UNIQUE AUTO_INCREMENT
  )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS SpeakerProfileTypes  (
  SpeakerType VARCHAR(50),
  Label VARCHAR(50),
  State VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  dr_id INTEGER UNIQUE AUTO_INCREMENT
  )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;
  

CREATE TABLE IF NOT EXISTS BillAnalysis(
    analysis_id DECIMAL(22, 0),
    bill_id VARCHAR(23),
    house VARCHAR(1),
    analysis_type VARCHAR(100),
    committee_code VARCHAR(6),
    committee_name VARCHAR(200),
    amendment_author VARCHAR(100),
    analysis_date DATETIME,
    amendment_date DATETIME,
    page_num DECIMAL(22, 0),
    source_doc LONGBLOB,
    released_floor VARCHAR(1),
    active_flg VARCHAR(1),
    trans_uid VARCHAR(20),
    trans_update DATETIME,
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
    dr_id INTEGER UNIQUE AUTO_INCREMENT

    PRIMARY KEY(analysis_id)
);


CREATE TABLE IF NOT EXISTS LegStaffGifts (
  year YEAR,
  agency_name VARCHAR(200), -- The broad gov agency receiving the gift
  staff_member INT, -- the staff member receiving the gift
  legislator INT, -- the legislator the staff member is associated with
  position VARCHAR(200),
  district_number INT,
  jurisdiction VARCHAR(200),
  source_name VARCHAR(200),
  source_city VARCHAR(200),
  source_state VARCHAR(200),
  source_business VARCHAR(200), -- business the source is involved in
  date_given DATE, 
  gift_value DECIMAL,
  reimbursed BOOLEAN, -- this one is just a flag
  gift_description VARCHAR(200),
  speech_or_panel BOOLEAN, -- flag to see if was for a speech
  image_url VARCHAR(2000),
  schedule ENUM('D', 'E'),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  FOREIGN KEY (staff_member) REFERENCES Person(pid),
  FOREIGN KEY  (legislator) REFERENCES Legislator(pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LegOfficePersonnel (
  staff_member INT,
  legislator INT, -- pk for term
  term_year YEAR, -- pk for term
  house VARCHAR(100), -- pk for term 
  start_date DATE, -- When the staff member started at this office
  end_date DATE,  -- when staff member ended with that office
  title VARCHAR(100),
  state CHAR(2), -- pk for term 
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (staff_member, legislator, term_year), 
  FOREIGN KEY (staff_member) REFERENCES LegislativeStaff(pid), 
  FOREIGN KEY (legislator, term_year, house, state) 
    REFERENCES Term(pid, year, house, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE IF NOT EXISTS LegislatureOffice (
  lo_id INT AUTO_INCREMENT,
  name VARCHAR(200),
  house VARCHAR(200),
  state CHAR(2), -- pk for term 
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

  PRIMARY KEY (lo_id),
  UNIQUE (name, house, state),
  FOREIGN KEY (house, state) REFERENCES House(name, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


CREATE TABLE IF NOT EXISTS OfficePersonnel (
  staff_member INT,
  office INT, -- pk for term
  start_date DATE, -- When the staff member started at this office
  end_date DATE,  -- when staff member ended with that office
  state CHAR(2), -- pk for term 
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (staff_member, office, start_date), 
  FOREIGN KEY (staff_member) REFERENCES LegislativeStaff(pid), 
  FOREIGN KEY (office) REFERENCES LegislatureOffice(lo_id)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/* Following 3 tables are dropped and re-added each night. Should probably be views,
   but we want the imports to run quickly.
 */
CREATE TABLE OrgAlignments (
  oid int(11) DEFAULT NULL,
  bid varchar(23) CHARACTER SET utf8 DEFAULT NULL,
  hid int(11) DEFAULT NULL,
  alignment char(20) CHARACTER SET utf8 DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE LegParticipation
(
    hid INT(11),
    did INT(11) DEFAULT '0',
    bid VARCHAR(23),
    pid INT(11),
    first VARCHAR(50) NOT NULL,
    last VARCHAR(50) NOT NULL,
    party ENUM('Republican', 'Democrat', 'Other'),
    LegBillWordCount DECIMAL(32) DEFAULT '0' NOT NULL,
    LegBillTime DECIMAL(33) DEFAULT '0' NOT NULL,
    LegBillPercentWord DECIMAL(36,4) DEFAULT '0.0000' NOT NULL,
    LegBillPercentTime DECIMAL(37,4) DEFAULT '0.0000' NOT NULL,
    LegHearingWordCount DECIMAL(32) DEFAULT '0' NOT NULL,
    LegHearingTime DECIMAL(33) DEFAULT '0' NOT NULL,
    LegHearingPercentWord DECIMAL(36,4) DEFAULT '0.0000' NOT NULL,
    LegHearingPercentTime DECIMAL(37,4) DEFAULT '0.0000' NOT NULL,
    LegHearingAvg DECIMAL(58,4) DEFAULT '0.0000' NOT NULL,
    BillWordCount DECIMAL(54) DEFAULT '0' NOT NULL,
    HearingWordCount DECIMAL(54) DEFAULT '0' NOT NULL
);

CREATE TABLE LegAvgPercentParticipation
(
    pid INT(11),
    first VARCHAR(50) NOT NULL,
    last VARCHAR(50) NOT NULL,
    AvgPercentParticipation DECIMAL(58,4)
);

/* Entity::DeprecatedPerson

   This is used for tracking what people are deprecated and will flush them 
   out at a set time.

   Used by: Toshi
*/
CREATE TABLE IF NOT EXISTS DeprecatedPerson(
    pid INTEGER,     -- Person id (ref. Person.pid)
    
    PRIMARY KEY(pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::DeprecatedOrganizations

   This is used for tracking what Organizations are deprecated

   Used by: Toshi
*/
CREATE TABLE IF NOT EXISTS DeprecatedOrganization(
   oid INTEGER,      -- Organization id (ref. Organization.oid)
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY(oid)
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
   hid INTEGER ,    -- added
   did INTEGER , 
   editor_id INTEGER ,
   name VARCHAR(1000) NOT NULL , 
   vid INTEGER , 
   startTime INTEGER NOT NULL , 
   endTime INTEGER NOT NULL , 
   created DATE,
   assigned DATE, 
   completed DATE,
   priority INT,
   
   PRIMARY KEY (tid),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did),
   FOREIGN KEY (editor_id) REFERENCES TT_Editor(id),
   FOREIGN KEY (vid) REFERENCES Video(vid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE TT_EditorStates (
  tt_user INT,
  state VARCHAR(2),
  priority INT,

  PRIMARY KEY (tt_user, state),
  FOREIGN KEY (tt_user) REFERENCES TT_Editor(id),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS TT_Videos (
   videoId INTEGER AUTO_INCREMENT,
   hearingName VARCHAR(255),
   hearingDate DATE,
   url VARCHAR(255), 
   sourceUrl VARCHAR(255), 
   fileName VARCHAR(255),
   duration FLOAT,
   state VARCHAR(2),
   status ENUM("downloading","downloaded","failed","skipped","queued","diarized","cut","approved","tasked"),
   glacierId VARCHAR(255),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   PRIMARY KEY (videoId)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS TT_Cuts (
   cutId INTEGER AUTO_INCREMENT,
   videoId INTEGER,
   fileId VARCHAR(255),
   fileName VARCHAR(255),
   start_time FLOAT,
   end_time FLOAT,
   leading_silence FLOAT DEFAULT 0.0,
   type ENUM("pause","silenece"),
   finalized BOOLEAN NOT NULL,
   current BOOLEAN NOT NULL,
   created TIMESTAMP DEFAULT NOW(),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   PRIMARY KEY (cutId),
   FOREIGN KEY (videoId) REFERENCES TT_Videos(videoId)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS TT_ServiceRequests (
   cutId INTEGER,
   serviceProvider ENUM("cielo", "green_button", "other"),
   turnaround INTEGER,
   fidelity VARCHAR(255),
   importance VARCHAR(255),
   transcript VARCHAR(255),
   job_id VARCHAR(255),
   status ENUM("in_progress", "completed") DEFAULT "in_progress", 
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   PRIMARY KEY (cutId),
   FOREIGN KEY (cutId) REFERENCES TT_Cuts(cutId)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

  CREATE TABLE IF NOT EXISTS TT_HostingUrl (
     cutId INTEGER,
     url VARCHAR(255),
     lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
     PRIMARY KEY (cutId),
     FOREIGN KEY (cutId) REFERENCES TT_Cuts(cutId)
  )
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE OR REPLACE VIEW TT_currentCuts 
AS SELECT * FROM TT_Cuts 
WHERE current = TRUE AND finalized = FALSE ORDER BY videoId DESC, cutId ASC;




/* The following are all dead tables. */

-- CREATE TABLE IF NOT EXISTS JobSnapshot (
--    pid   INTEGER,
--    hid   INTEGER,
--    role  ENUM('Lobbyist', 'General_public', 'Legislative_staff_commitee', 'Legislative_staff_author', 'State_agency_rep', 'Unknown'),
--    employer VARCHAR(50), -- employer: lobbyist: lobying firm, union, corporation. SAR: name of Agency/Department. GP: teacher/etc.
--    client   VARCHAR(50), -- client: only for lobbyist

--    PRIMARY KEY (pid, hid),
--    FOREIGN KEY (pid) REFERENCES Person(pid),
--    FOREIGN KEY (hid) REFERENCES Hearing(hid)
-- )
-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;

-- CREATE TABLE IF NOT EXISTS attends (
--    pid    INTEGER,
--    hid    INTEGER,

--    PRIMARY KEY (pid, hid),
--    FOREIGN KEY (pid) REFERENCES Legislator(pid), -- Person
--    FOREIGN KEY (hid) REFERENCES Hearing(hid)
-- )
-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;

-- tag is a keyword. For example, "education", "war on drugs"
-- can also include abbreviations for locations such as "Cal Poly" for "Cal Poly SLO"
-- CREATE TABLE IF NOT EXISTS tag (
--    tid INTEGER AUTO_INCREMENT
-- ,   tag VARCHAR(50),

--    PRIMARY KEY (tid)
-- )
-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;

-- -- join table for Uterrance >>> Tag
-- CREATE TABLE IF NOT EXISTS join_utrtag (
--    uid INTEGER,
--    tid INTEGER,

--    PRIMARY KEY (uid, tid),
--    FOREIGN KEY (tid) REFERENCES tag(tid),
--    FOREIGN KEY (uid) REFERENCES Utterance(uid)
-- )
-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;

-- -- an utterance might contain an honorific or a pronoun where it is unclear who the actual person is
-- -- this is a "mention" and should be joined against when searching for a specific person 
-- CREATE TABLE IF NOT EXISTS Mention (
--    uid INTEGER,
--    pid INTEGER,

--    PRIMARY KEY (uid, pid),
--    FOREIGN KEY (pid) REFERENCES Person(pid),
--    FOREIGN KEY (uid) REFERENCES Utterance(uid)
-- )
-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;

-- CREATE TABLE IF NOT EXISTS user (
--    email VARCHAR(255) NOT NULL,
--    name VARCHAR(255),
--    password VARCHAR(255) NOT NULL,
--    new_user INTEGER,

--    PRIMARY KEY (email)
-- )
-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;

-- CREATE TABLE IF NOT EXISTS BillDisRepresentation(
--     did INTEGER,
--     pid INTEGER,
--     oid INTEGER,
--     hid INTEGER,
    
--     PRIMARY KEY (did, pid, oid, hid),
--     FOREIGN KEY (did) REFERENCES BillDiscussion(did),
--     FOREIGN KEY (pid) REFERENCES Person(pid),
--     FOREIGN KEY (oid) REFERENCES LobbyistEmployer(oid),
--     FOREIGN KEY (hid) REFERENCES Hearing(hid)
-- )
-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;

-- CREATE TABLE IF NOT EXISTS TT_TaskCompletion (
--    tcid INTEGER AUTO_INCREMENT , 
--    tid INTEGER , 
--    completion DATE , 
   
--    PRIMARY KEY (tcid),
--    FOREIGN KEY (tid) REFERENCES TT_Task(tid)
-- )
-- ENGINE = INNODB
-- CHARACTER SET utf8 COLLATE utf8_general_ci;


