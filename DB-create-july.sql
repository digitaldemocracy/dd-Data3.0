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

/* Entity::Person

   Describes any person. Can be a Legislator, GeneralPublic, Lobbyist, etc.
*/
CREATE TABLE IF NOT EXISTS Person (
   pid    INTEGER AUTO_INCREMENT,   -- Person id
   last   VARCHAR(50) NOT NULL,     -- last name
   first  VARCHAR(50) NOT NULL,     -- first name
   image VARCHAR(256),              -- path to image (if exists)

   PRIMARY KEY (pid)
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
   room_number    INTEGER,       -- room number
   email_form_link VARCHAR(200), -- email link
   OfficialBio TEXT,             -- bio
   
   PRIMARY KEY (pid),
   FOREIGN KEY (pid) REFERENCES Person(pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Weak Entity::Term

   Legislators have Terms. For each term a legislator serves, keep track of 
   what district, house, and party they are associated with because legislators 
   can change those every term.
*/
CREATE TABLE IF NOT EXISTS Term (
   pid      INTEGER,    -- Person id (ref. Person.pid)
   year     YEAR,       -- year served
   district INTEGER(3), -- district legislator served in
   house    ENUM('Assembly', 'Senate') NOT NULL,
   party    ENUM('Republican', 'Democrat', 'Other') NOT NULL,
   start    DATE,       -- start date of term
   end      DATE,       -- end date of term

   PRIMARY KEY (pid, year, district, house),
   FOREIGN KEY (pid) REFERENCES Legislator(pid) -- change to Person
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
   house  ENUM('Assembly', 'Senate', 'Joint') NOT NULL,
   name   VARCHAR(200) NOT NULL,    -- committee name
   Type   ENUM('Standing','Select','Budget Subcommittee','Joint'),

   PRIMARY KEY (cid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Relationship::servesOn(many-to-many) << [Committee, Term]

   A legislator (in a specific term) can serve on one or more committees.
*/
CREATE TABLE IF NOT EXISTS servesOn (
   pid      INTEGER,                               -- Person id (ref. Person.pid)
   year     YEAR,                                  -- year served
   district INTEGER(3),                            -- district served
   house    ENUM('Assembly', 'Senate') NOT NULL,   -- house served
   cid      INTEGER(3),                            -- Committee id (ref. Committee.cid)

   PRIMARY KEY (pid, year, district, house, cid),
   FOREIGN KEY (pid, year, district, house) REFERENCES Term(pid, year, district, house),
   FOREIGN KEY (cid) REFERENCES Committee(cid)
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
   bid     VARCHAR(20),          -- Bill id (concat of years+session+type+number)
   type    VARCHAR(3) NOT NULL,  -- bill type abbreviation
   number  INTEGER NOT NULL,     -- bill number
   state   ENUM('Chaptered', 'Introduced', 'Amended Assembly', 'Amended Senate', 'Enrolled',
      'Proposed', 'Amended', 'Vetoed') NOT NULL,
   status  VARCHAR(60),          -- current bill status
   house   ENUM('Assembly', 'Senate', 'Secretary of State', 'Governor', 'Legislature'),
   session INTEGER(1),           -- 0: Normal session, 1: Special session

   PRIMARY KEY (bid),
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

   PRIMARY KEY (hid)
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

    PRIMARY KEY (cid, hid),
    FOREIGN KEY (cid) REFERENCES Committee(cid),
    FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/* This table is deprecated */
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
   srtFlag TINYINT(1) DEFAULT 0,

   PRIMARY KEY (vid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Video_ttml (
   vid INTEGER,
   version INTEGER DEFAULT 0,
   ttml MEDIUMTEXT,
   source VARCHAR(4) DEFAULT 0,

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
   doPass TINYINT(1)

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
   alignment ENUM('For', 'Against', 'For_if_amend', 'Against_unless_amend', 'Neutral', 'Indeterminate', 'NA'),
   dataFlag INTEGER DEFAULT 0,
   diarizationTag VARCHAR(5) DEFAULT '',
   did INT,

   PRIMARY KEY (uid, current),
   UNIQUE KEY (uid, vid, pid, current, time),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (vid) REFERENCES Video(vid),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did)
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
   tid INTEGER AUTO_INCREMENT
,   tag VARCHAR(50),

   PRIMARY KEY (tid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- join table for Uterrance >>> Tag
CREATE TABLE IF NOT EXISTS join_utrtag (
   uid INTEGER,
   tid INTEGER,

   PRIMARY KEY (uid, tid),
   FOREIGN KEY (tid) REFERENCES tag(tid),
   FOREIGN KEY (uid) REFERENCES Utterance(uid)
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
    voteId      INTEGER AUTO_INCREMENT,
    bid     VARCHAR(20),
    mid     INTEGER(20),
    cid     INTEGER, 
    VoteDate    DATETIME,
    ayes        INTEGER,
    naes        INTEGER,
    abstain     INTEGER,
    result      VARCHAR(20),
    
    PRIMARY KEY(voteId),
    FOREIGN KEY (mid) REFERENCES Motion(mid),
    FOREIGN KEY (bid) REFERENCES Bill(bid),
    FOREIGN KEY (cid) REFERENCES Committee(cid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS BillVoteDetail (
    pid     INTEGER,
    voteId  INTEGER,
    result  VARCHAR(20),
    
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
   hid INTEGER ,    -- added
   did INTEGER , 
   editor_id INTEGER ,
   name VARCHAR(255) NOT NULL , 
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

/* Entity::Organizations

   Organizations are companies or organizations.
*/
CREATE TABLE IF NOT EXISTS Organizations(
    oid INTEGER AUTO_INCREMENT,  -- Organization id
    name VARCHAR(200),           -- name
    type INTEGER DEFAULT 0,      -- type (not fleshed out yet)
    city VARCHAR(200),           -- city
    state VARCHAR(2),            -- U.S. state

    PRIMARY KEY(oid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS LobbyistEmployer(
   filer_id VARCHAR(9),  -- modified (PK)
   oid INTEGER,
   coalition TINYINT(1),
   
   PRIMARY KEY (oid),
   FOREIGN KEY (oid) REFERENCES Organizations(oid)
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
   oid INTEGER, -- modified (renamed)
   hearing_date DATE,                                       -- modified (renamed)
   hid INTEGER,              -- added
   did INTEGER,

   PRIMARY KEY(pid, oid, hid, did),                 -- added
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (oid) REFERENCES LobbyistEmployer(oid),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did)
);

CREATE TABLE IF NOT EXISTS GeneralPublic(
   pid INTEGER,   -- added
   position VARCHAR(100),
   RecordId INTEGER AUTO_INCREMENT,  
   hid   INTEGER,                       -- added
   did INTEGER,
   oid INTEGER, 

   PRIMARY KEY (RecordId),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did),
   FOREIGN KEY (oid) REFERENCES Organizations(oid)
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
   hid   INTEGER,                       -- added

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
   hid   INTEGER,                       -- added

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
   hid   INTEGER,                       -- added

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
    oid INTEGER,
    hid INTEGER,
    
    PRIMARY KEY (did, pid, oid, hid),
    FOREIGN KEY (did) REFERENCES BillDiscussion(did),
    FOREIGN KEY (pid) REFERENCES Person(pid),
    FOREIGN KEY (oid) REFERENCES LobbyistEmployer(oid),
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

   PRIMARY KEY(oid)
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
    state VARCHAR(2),         -- U.S. state

    PRIMARY KEY(prid)
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
    
    PRIMARY KEY(official, payor, payee, datePaid),
    FOREIGN KEY(official) REFERENCES Person(pid), 
    FOREIGN KEY(payor) REFERENCES Payors(prid),
    FOREIGN KEY(payee) REFERENCES Organizations(oid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS BillAnalysis(
    analysis_id DECIMAL(22, 0),
    bill_id VARCHAR(20),
    house VARCHAR(1),
    analysis_type VARCHAR(100),
    committee_code VARCHAR(6),
    committee_name VARCHAR(200),
    amendment_author VARCHAR(100),
    analysis_date DATETIME,
    amendment_date DATETIME,
    page_num DECIMAL(22, 0),
    source_doc LONGBLOG,
    released_floor VARCHAR(1),
    active_flg VARCHAR(1),
    trans_uid VARCHAR(20),
    trans_update DATETIME,

    PRIMARY KEY(analysis_id)
);
