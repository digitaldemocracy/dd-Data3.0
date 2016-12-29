/*
file: DB-setup.sql
authors: Daniel Mangin
         Mandy Chan
         Andrew Voorhees

Start Date: 6/26/2015
End Usage:

Description: Used to create all of the tables for Digital Democracy
note: this will only work on the currently used database

/*****************************************************************************/
/*
  Represents a state. e.g. California, Arizona

  Sources: None (Done manually)
*/
CREATE TABLE IF NOT EXISTS State (
  abbrev  VARCHAR(2),  -- eg CA, AZ
  country  VARCHAR(200), -- eg United States
  name   VARCHAR(200), -- eg Caliornia, Arizona
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  The start and end year for a specific legislative session. E.g. most recent CA session was
  2015-2016.

  Sources: None (Done manually)
*/
CREATE TABLE IF NOT EXISTS Session (
  state VARCHAR(2),
  start_year YEAR,
  end_year YEAR,
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (state, start_year, end_year),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Legislators are elected to represent specific districts.

  Sources: CA: refactored_Get_Districts.py
           NY: ny_import_districts.py
 */
CREATE TABLE IF NOT EXISTS District (
  state VARCHAR(2),
  house VARCHAR(100),
  did INTEGER,
  note VARCHAR(40) DEFAULT '',
  year INTEGER,
  region TEXT,
  geoData MEDIUMTEXT,
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY(state, house, did, year),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  FOREIGN KEY (house, state) REFERENCES House(name, state)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  A house in a legislature. Necessary because different states can have
  different names for their houses.

  Sources: None (Done manually)
*/
CREATE TABLE IF NOT EXISTS House (
  name  VARCHAR(100), -- Name for the house. eg Assembly, Senate
  state VARCHAR(2),
  type VARCHAR(100),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/* Entity::Person

   Describes any person. Can be a Legislator, GeneralPublic, Lobbyist, etc.

   Sources: CA: Transcription Tool,
                refactored_legislator_migrate.py, refactored_Contributions.py,
                refactored_Get_Committees_Web.py, refactored_Cal-Access-Accessor.py
            NY: Transcription Tool,
                ny_import_legislators.py, ny_import_lobbyists.py
*/
CREATE TABLE IF NOT EXISTS Person (
  pid    INTEGER AUTO_INCREMENT,   -- Person id
  last   VARCHAR(50) NOT NULL,     -- last name
  middle VARCHAR(50),              -- middle name
  first  VARCHAR(50) NOT NULL,     -- first name
  source VARCHAR(255),
  image VARCHAR(256),              -- path to image (if exists)
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (pid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  Used to capture the affiliation between a person and a state.

  Sources: CA: Transcription Tool,
               refactored_legislator_migrate.py, refactored_Contributions.py,
               refactored_Get_Committees_Web.py, refactored_Cal-Access-Accessor.py
           NY: Transcription Tool,
               ny_import_legislators.py, ny_import_lobbyists.py
*/
CREATE TABLE IF NOT EXISTS PersonStateAffiliation (
  pid    INTEGER,
  state  VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (pid, state),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::Organizations

   Organizations are companies or organizations.

   Sources: CA: Transcription Tool,
                refactored_Contributions.py, refactored_Cal-Access-Accessor.py
            NY: Transcription Tool,
                ny_import_lobbyist.py
*/
CREATE TABLE IF NOT EXISTS Organizations (
  oid INTEGER AUTO_INCREMENT,  -- Organization id
  name VARCHAR(200),           -- name
  type INTEGER DEFAULT 0,      -- type (not fleshed out yet)
  city VARCHAR(200),           -- city
  stateHeadquartered VARCHAR(2), -- U.S. state, where it's based
  analysis_flag BOOL DEFAULT FALSE,
  source VARCHAR(255),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (oid),
  FOREIGN KEY (stateHeadquarterd) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  Holds information about merged organizations. We
  use this because we want to retain the names of Organizations
  we think should be considered the same.
 */
CREATE TABLE IF NOT EXISTS MergedOrgs (
  oid INT,
  merged_name VARCHAR(255),

  PRIMARY KEY (oid, merged_name),
  FOREIGN KEY (oid) REFERENCES Organizations(oid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
    Tracks the relationship between Organizations and their affiliated states.

   Sources: CA: Transcription Tool,
                refactored_Contributions.py, refactored_Cal-Access-Accessor.py
            NY: Transcription Tool,
                ny_import_lobbyist.py
*/
CREATE TABLE IF NOT EXISTS OrganizationStateAffiliation (
  oid    INTEGER,
  state  VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (oid, state),
  FOREIGN KEY (oid) REFERENCES Organizations(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::Legislator

   A legislator has a description and bio and several contact information.

   Sources: CA: refactored_legislator_migrate.py
            NY: ny_import_legislators.py, ny_import_spreadsheet_data.py
*/
CREATE TABLE IF NOT EXISTS Legislator (
  pid         INTEGER,          -- Person id (ref. Person.pid)
  description VARCHAR(1000),    -- description
  twitter_handle VARCHAR(100),  -- twitter handle (ex: @example)
  capitol_phone  VARCHAR(30),   -- phone number (format: (xxx) xxx-xxxx)
  website_url    VARCHAR(200),  -- url
  room_number    VARCHAR(10),       -- room number
  email_form_link VARCHAR(200), -- email link
  state    VARCHAR(2), -- state where term was served
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, state),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/* Weak Entity::Term:

   Legislators have Terms. For each term a legislator serves, keep track of
   what district, house, and party they are associated with because legislators
   can change those every term.

   Sources: CA: refactored_legislator_migrate.py
            NY: ny_import_legislators.py
*/
CREATE TABLE IF NOT EXISTS Term (
  pid      INTEGER,    -- Person id (ref. Person.pised)
  official_bio TEXT,
  year     YEAR,       -- year served
  district INTEGER(3), -- district legislator served in
  house    VARCHAR(100), -- house they serve in,
  party    ENUM('Republican', 'Democrat', 'Other'),
  start    DATE,       -- start date of term
  start_ts INT(11) AS (UNIX_TIMESTAMP(start)), -- Used by Drupal
  end      DATE,       -- end date of term
  end_ts INT(11) AS (UNIX_TIMESTAMP(end)), -- Used by Drupal
  current_term TINYINT(4), -- Whether this is a current term
  state    VARCHAR(2), -- state where term was served
  -- caucus   VARCHAR(200), -- group that generally votes together. Not currently in use
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
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

   Sources: CA: refractored_Get_Committees_Web.py
            NY: ny_import_committees.py, ny_import_committees_scrape_assembly.py
*/
CREATE TABLE IF NOT EXISTS Committee (
  cid    INTEGER(3),               -- Committee id
  session_year YEAR,
  current_flag BOOL,
  house  VARCHAR(200) NOT NULL,
  name   VARCHAR(200) NOT NULL,    -- committee name
  short_name   VARCHAR(200) NOT NULL,    -- committee name
  type   VARCHAR(100),
  room VARCHAR(255),
  phone VARCHAR(30),
  fax VARCHAR(30),
  email VARCHAR(256),
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (cid),
  UNIQUE (session_year, house, name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  FOREIGN KEY (house, state) REFERENCES House(name, state)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Relationship::servesOn(many-to-many) << [Committee, Term]

   A legislator (in a specific term) can serve on one or more committees.

   Sources: CA: refractored_Get_Committees_Web.py
            NY: ny_import_committees.py, ny_import_committees_scrape_assembly.py
*/
CREATE TABLE IF NOT EXISTS servesOn (
  pid      INTEGER,                               -- Person id (ref. Person.pid)
  year     YEAR,                                  -- The session year
  house    VARCHAR(100),
  cid      INTEGER(3),                            -- Committee id (ref. Committee.cid)
  position ENUM('Chair', 'Vice-Chair', 'Co-Chair', 'Member'),
  current_flag BOOL,
  start_date DATE,
  end_date DATE,
  state    VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, year, house, state, cid),
  FOREIGN KEY (cid) REFERENCES Committee(cid),
  FOREIGN KEY (house, state) REFERENCES House(name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Similar to servesOn, but the relationship between staff members and committees.

  Sources: TODO write a script that does this
 */
CREATE TABLE IF NOT EXISTS ConsultantServesOn (
  pid      INTEGER,                               -- Person id (ref. Person.pid)
  session_year     YEAR,                                  -- year served
  cid      INTEGER(3),                            -- Committee id (ref. Committee.cid)
  position ENUM('Chief Consultant', 'Committee Secretary', 'Deputy Chief Consultant'),
  current_flag BOOL,
  start_date DATE,
  start_date_ts INT,
  end_date DATE DEFAULT NULL,
  end_date_ts INT,
  state    VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, cid, start_date),
  FOREIGN KEY (cid) REFERENCES Committee(cid),
  FOREIGN KEY (pid) REFERENCES LegislativeStaff(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/* Entity::Bill

   A legislator (Senator/Assembly Member) or Committee can author a bill. It
   goes through the legislative process and changes states and versions multiple
   times. The house is where the bill was introduced in. The session indicates
   what legislative session was occurring when the bill was introduced.

   Sources: CA: refactored_Bill_Extract
            NY: ny_import_bills.py
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
  visibility_flag BOOLEAN DEFAULT 0,
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (bid),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  INDEX name (type, number)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Bills go through different versions. This table captures a specific version of a bill. Vids do
  not necessarily follow a logical order, but they do always contain the bid

  Sources: CA: refactored_Bill_Extract.py
           NY: ny_import_bills.py
 */
CREATE TABLE IF NOT EXISTS BillVersion (
  vid                 VARCHAR(33),
  bid                 VARCHAR(23),
  date                DATE,
  date_ts  INT(11) AS (UNIX_TIMESTAMP(date_ts)), -- Used by Drupal
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
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (vid),
  FOREIGN KEY (bid) REFERENCES Bill(bid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::Hearing

   There are many hearings per day. A bill is presented during a hearing and
   testimonies may be heard in support or opposition to the bill. During the
   hearing, a committee will vote on the bill.

   Sources: CA: Transcription Tool,
                ca_agenda.py
            NY: Transcription Tool
*/
CREATE TABLE IF NOT EXISTS Hearing (
  hid    INTEGER AUTO_INCREMENT,      -- Hearing id
  date   DATE,                        -- date of hearing
  date_ts INT(11) AS (UNIX_TIMESTAMP(date)), -- Used by Drupal
  type ENUM('Regular', 'Budget', 'Informational', 'Summary') DEFAULT 'Regular',
  session_year YEAR,
  state  VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),


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

   Sources: CA: ca_agenda.py
            NY: Transcription Tool
*/
CREATE TABLE IF NOT EXISTS CommitteeHearings (
  cid INTEGER,  -- Committee id (ref. Committee.cid)
  hid INTEGER,  -- Hearing id (ref. Hearing.hid)
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (cid, hid),
  FOREIGN KEY (cid) REFERENCES Committee(cid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
   Used to hold the many-to-many relationship btw Hearings and Bills.

   Sources: CA: ca_agenda.py
            NY: No data
*/
CREATE TABLE IF NOT EXISTS HearingAgenda (
  hid INTEGER,  -- Hearing id (ref. Hearing.hid)
  bid VARCHAR(23),
  date_created DATE, -- The date the agenda info was posted
  date_created_ts INT(11) AS (UNIX_TIMESTAMP(date_created)), -- Used by Drupal
  current_flag TINYINT(1), -- Whether this is the most recent agenda
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (hid, bid, date_created),
  FOREIGN KEY (bid) REFERENCES Bill(bid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Actions that take place on bills. E.g. "first time read", "Moved to floor"

   Sources: CA: refactored_Action_Extract_Aug.py
            NY: ny_import_actions.py
 */
CREATE TABLE IF NOT EXISTS Action (
  bid    VARCHAR(23),
  date   DATE,
  date_ts   INT(11) AS (UNIX_TIMESTAMP(date)), -- Used by Drupal
  text   TEXT,
  seq_num INT,
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  FOREIGN KEY (bid) REFERENCES Bill(bid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Info for the videos of the hearings hosted on the site.

  Sources: CA: Transcription Tool
           NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS Video (
  vid INTEGER AUTO_INCREMENT,
  fileId VARCHAR(50), -- formerly youtubeId. Our name for file
  hid INTEGER,
  position INTEGER,
  startOffset INTEGER,
  duration INTEGER,
  srtFlag TINYINT(1) DEFAULT 0,
  state VARCHAR(2),
  source ENUM("YouTube", "Local", "Other"),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (vid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  Used for diarization of Vidoes

  Sources: CA: Transcription Tool
           NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS Video_ttml (
  vid INTEGER,
  version INTEGER DEFAULT 0,
  ttml MEDIUMTEXT,
  source VARCHAR(4) DEFAULT 0,
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  FOREIGN KEY (vid) REFERENCES Video(vid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  The portion of a Hearing where a specific bill was discussed.

  Sources: CA: Transcription Tool
           NY: Transcription Tool
 */
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
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (did),
  UNIQUE KEY (bid, startVideo, startTime),
  FOREIGN KEY (bid) REFERENCES Bill(bid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid),
  FOREIGN KEY (startVideo) REFERENCES Video(vid),
  FOREIGN KEY (endVideo) REFERENCES Video(vid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  Specific motions on bills. E.g. "Motion to ammend", "Motion to table"

  Sources:  CA: refactored_Motion_Extract.py
            NY: No Data
 */
CREATE TABLE IF NOT EXISTS Motion (
  mid    INTEGER(20),
  text   TEXT,
  doPass TINYINT(1),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (mid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  The aggregated vote information for a specific vote on a bill.

  Sources: CA: refactored_Vote_Extract.py
           NY: ny_import_billvotes.py
 */
CREATE TABLE IF NOT EXISTS BillVoteSummary (
  voteId      INTEGER AUTO_INCREMENT,
  bid     VARCHAR(23),
  mid     INTEGER(20),
  cid     INTEGER,
  VoteDate    DATETIME,
  VoteDate_ts INT(11) AS (UNIX_TIMESTAMP(VoteDate)), -- Used by Drupal
  ayes        INTEGER,
  naes        INTEGER,
  abstain     INTEGER,
  result      VARCHAR(20),
  VoteDateSeq INT,
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY(voteId),
  FOREIGN KEY (mid) REFERENCES Motion(mid),
  FOREIGN KEY (bid) REFERENCES Bill(bid),
  FOREIGN KEY (cid) REFERENCES Committee(cid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  The vote information of a specific legislators on a given vote.

  Sources: CA: refactored_Vote_Extract.py
           NY: ny_import_billvotes.py
 */
CREATE TABLE IF NOT EXISTS BillVoteDetail (
  pid     INTEGER,
  voteId INTEGER,
  result  VARCHAR(20),
  state   VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY(pid, voteId),
  FOREIGN KEY (pid) REFERENCES Legislator(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  FOREIGN KEY (voteId) REFERENCES BillVoteSummary(voteId)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
   The legislators that author specific bills.

   Sources: CA: refactored_Author_Extract.py
            NY: ny_import_authors.py

   **Note that for now contribution is either always lead author
   or blank
 */
CREATE TABLE IF NOT EXISTS authors (
  pid          INTEGER,
  bid          VARCHAR(23),
  vid          VARCHAR(33),
  contribution ENUM('Lead Author', 'Principal Coauthor', 'Coauthor') DEFAULT 'Coauthor',
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, bid, vid),
  FOREIGN KEY (pid) REFERENCES Legislator(pid), -- change to Person
  FOREIGN KEY (bid, vid) REFERENCES BillVersion(bid, vid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  This is just the different author rolls enumerated. So why isn't it an enum Andrew?
  Don't ask too many questions.

  Sources: CA: TODO something should fill this
           NY: No data
 */
CREATE TABLE IF NOT EXISTS BillSponsorRolls (
  roll VARCHAR(100),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (roll)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  Table basically just the same info as authors, but it clarifies their
  role. We have a second table as not to confuse the druple scripts that
  pull author names. Ideally we role this into authors soon.

  Sources: CA: TODO something should fill this
           NY: No data
 */
CREATE TABLE IF NOT EXISTS BillSponsors (
  pid          INTEGER,
  bid          VARCHAR(23),
  vid          VARCHAR(33),
  contribution VARCHAR(100),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, bid, vid, contribution),
  FOREIGN KEY (pid) REFERENCES Legislator(pid),
  FOREIGN KEY (bid, vid) REFERENCES BillVersion(bid, vid),
  FOREIGN KEY (contribution) REFERENCES BillSponsorRolls(roll)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Bills can be authored by legislators or whole committees. This table captures the case where
  a bill was authored by a committee.

  Sources: CA: refactored_Author_Extract.py
           NY: ny_import_committeeauthors.py
 */
CREATE TABLE IF NOT EXISTS CommitteeAuthors(
  cid INTEGER,
  bid VARCHAR(23),
  vid VARCHAR(33),
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY(cid, bid, vid),
  FOREIGN KEY (bid) REFERENCES Bill(bid),
  FOREIGN KEY (cid) REFERENCES Committee(cid),
  FOREIGN KEY (vid) REFERENCES BillVersion(vid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  The individual blocks of text our transcripts are broken up into.

  Sources: CA: Transcription Tool
           NY: Transcription Tool
 */
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
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (uid),
  UNIQUE KEY (uid, vid, pid, current, time),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (vid) REFERENCES Video(vid),
  FOREIGN KEY (did) REFERENCES BillDiscussion(did),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;



/*
  Whenever an organization gives something to a legislator, that must be recorded. This table
  keeps track of all those "gifts".

  Sources: CA: TODO ??
           NY: TODO ??
 */
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
  giftDate_ts INT(11) AS (UNIX_TIMESTAMP(giftDate)), -- Used by Drupal
  sessionYear YEAR,
  reimbursed TINYINT(1),
  giftIncomeFlag TINYINT(1) DEFAULT 0,
  speechFlag TINYINT(1) DEFAULT 0,
  description VARCHAR(80),
  oid INT, -- Just matched from sourceName
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY(RecordId),
  FOREIGN KEY (oid) REFERENCES Organizations(oid),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  These are formal contributions to legislators campaigns.

  Sources: CA: refactored_Contributions.py
           NY: TODO ??
 */
CREATE TABLE IF NOT EXISTS Contribution (
  id VARCHAR(20),
  pid INTEGER,
  year INTEGER,
  date DATETIME,
  date_ts INT(11) AS (UNIX_TIMESTAMP(date)), -- Used by Drupal
  sesssionYear YEAR,
  house VARCHAR(10),
  donorName VARCHAR(255),
  donorOrg VARCHAR(255),
  amount DOUBLE,
  oid INT, -- just matched from donorOrg
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY(id),
  FOREIGN KEY (oid) REFERENCES Organizations(oid),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Lobbyists are people who "lobby" on behalf of organizations in front of Congress.

  Sources: CA: refactored_Cal-Access-Accessor.py
           NY: ny_import_lobbyists.py
 */
CREATE TABLE IF NOT EXISTS Lobbyist(
  pid INTEGER,   -- added
  -- FILER_NAML VARCHAR(50),               modified, needs to be same as Person.last
  -- FILER_NAMF VARCHAR(50),               modified, needs to be same as Person.first
  filer_id VARCHAR(200) UNIQUE,         -- modified, start with state prefix
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, state),                    -- added
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Similar to a law firm. Organizations hire lobbyists through a lobbying firm in
  order to represent the organization at a hearing.

  Sources: CA: refactored_Cal-Access-Accessor.py
           NY: ny_import_lobbyists.py
 */
CREATE TABLE IF NOT EXISTS LobbyingFirm(
  filer_naml VARCHAR(200),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (filer_naml)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  It is possible that a lobbying firm might be present in multiple states. This table represents the
  state specific info we have a firm in a given state.

  Sources: CA: refactored_Cal-Access-Accessor.py
           NY: ny_import_lobbyists.py
*/
CREATE TABLE IF NOT EXISTS LobbyingFirmState (
  filer_id VARCHAR(200),  -- modified, given by state
  rpt_date DATE,
  rpt_date_ts INT(11) AS (UNIX_TIMESTAMP(rpt_date)), -- Used by Drupal
  ls_beg_yr INTEGER,    -- modified (INT)
  ls_end_yr INTEGER,     -- modified (INT)
  filer_naml VARCHAR(200),
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (filer_id, state),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  FOREIGN KEY (filer_naml) REFERENCES LobbyingFirm(filer_naml)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  A organization that hires lobbyists on their behalf.

  Sources: CA: refactored_Cal-Access-Accessor.py
           NY: ny_import_lobbyists.py
 */
CREATE TABLE IF NOT EXISTS LobbyistEmployer (
   filer_id VARCHAR(200),  -- modified (PK)
   oid INTEGER,
   coalition TINYINT(1),
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (oid, state),
   UNIQUE (filer_id, state),
   FOREIGN KEY (oid) REFERENCES Organizations(oid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Records which lobbying firms employ which lobbyists.

  Sources: CA: refactored_Cal-Access-Accessor.py
           NY: ny_import_lobbyists.py
 */
CREATE TABLE IF NOT EXISTS LobbyistEmployment (
  pid INT,                         -- modified (FK)
  sender_id VARCHAR(200),
  rpt_date DATE,
  rpt_date_ts INT(11) AS (UNIX_TIMESTAMP(rpt_date)), -- Used by Drupal
  ls_beg_yr INTEGER,    -- modified (INT)
  ls_end_yr INTEGER,    -- modified (INT)
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, sender_id, rpt_date, ls_end_yr), -- modified (May 21)
  FOREIGN KEY (sender_id, state) REFERENCES LobbyingFirmState(filer_id, state),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Represents an instance where a lobbyist employer (an organization) hires a lobbyists directly,
  as opposed to hiring said lobbyists through a lobbying firm. Ie these are in house lobbyists

  Sources: CA: refactored_Cal-Access-Accessor.py
           NY: ny_import_lobbyists.py
 */
CREATE TABLE IF NOT EXISTS LobbyistDirectEmployment(
   pid INT,
   lobbyist_employer INTEGER,
   rpt_date DATE,
   rpt_date_ts INT(11) AS (UNIX_TIMESTAMP(rpt_date)), -- Used by Drupal
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,     -- modified (INT)
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (pid, lobbyist_employer, rpt_date, ls_end_yr, state), -- modified (May 21)
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (lobbyist_employer, state) REFERENCES LobbyistEmployer(oid, state)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Tracks the employment contracts between an organization and a lobbying firm. Ie when an organization
  hires a lobbying firm to use some of its lobbyists on that organizations behalf.

  Sources: CA: refactored_Cal-Access-Accessor.py
           NY: ny_import_lobbyists.py
 */
CREATE TABLE IF NOT EXISTS LobbyingContracts(
   filer_id VARCHAR(200),
   lobbyist_employer INTEGER, -- modified (FK)
   rpt_date DATE,
   rpt_date_ts INT(11) AS (UNIX_TIMESTAMP(rpt_date)), -- Used by Drupal
   ls_beg_yr INTEGER,    -- modified (INT)
   ls_end_yr INTEGER,     -- modified (INT)
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY (filer_id, lobbyist_employer, rpt_date, state), -- modified (May 21)
   FOREIGN KEY (lobbyist_employer, state) REFERENCES LobbyistEmployer(oid, state),
   FOREIGN KEY (filer_id, state) REFERENCES LobbyingFirmState(filer_id, state),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  A specific instance of a lobbyist speaking at a hearing on an organizationss behalf.

  Sources: CA: Transcription Tool
           NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS LobbyistRepresentation (
   pid INTEGER,
   oid INTEGER, -- modified (renamed)
   hearing_date DATE,                                       -- modified (renamed)
   hearing_date_ts INT(11) AS (UNIX_TIMESTAMP(hearing_date)), -- Used by Drupal
   hid INTEGER,              -- added
   did INTEGER,
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
   dr_id INTEGER UNIQUE AUTO_INCREMENT,

   PRIMARY KEY(pid, oid, hid, did),                 -- added
   FOREIGN KEY (pid) REFERENCES Lobbyist(pid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (oid, state) REFERENCES LobbyistEmployer(oid, state),
   FOREIGN KEY (did) REFERENCES BillDiscussion(did),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  A specific instance of a member of the general public speaking at a hearing on an organization's behalf.

  Sources: CA: Transcription Tool
           NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS GeneralPublic(
  pid INTEGER,   -- added
  position VARCHAR(100),
  RecordId INTEGER AUTO_INCREMENT,
  hid   INTEGER,                       -- added
  did INTEGER,
  oid INTEGER,
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (RecordId),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid),
  FOREIGN KEY (did) REFERENCES BillDiscussion(did),
  FOREIGN KEY (oid) REFERENCES Organizations(oid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  A person who works in state legislature for a legislator or specific committee.

  Sources: CA: Transcription Tool,
               ImportLegStaffGifts (whole side project)
           NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS LegislativeStaff (
  pid INTEGER,   -- added
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid),                    -- added
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  An instance of a legislative staff member speaking at a hearing.

  Sources: CA: Transcription Tool
           NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS LegislativeStaffRepresentation (
  pid INTEGER,   -- added
  flag TINYINT(1),  -- if flag is 0, there must be a legislator; if flag is 1, there must be a committee
  legislator INTEGER, -- this is the legislator
  committee INTEGER,
  hid   INTEGER,                       -- added
  did INTEGER,
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, hid, did),                    -- added
  FOREIGN KEY (pid) REFERENCES LegislativeStaff(pid),
  FOREIGN KEY (legislator) REFERENCES Legislator(pid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid),
  FOREIGN KEY (committee) REFERENCES Committee(cid),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  FOREIGN KEY (did) REFERENCES BillDiscussion(did),
  CHECK (Legislator IS NOT NULL AND flag = 0 OR committee IS NOT NULL AND flag = 1)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  A person who works for the LAO. They are in theory an objective third party who examines the
  logistics of specific pieces of legislation.

  Source: CA: Transcription Tool
          NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS LegAnalystOffice(
  pid INTEGER REFERENCES Person(pid),
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid),                    -- added
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  An instance of an LAO member speaking at a hearing.

  Source: CA: Transcription Tool
          NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS LegAnalystOfficeRepresentation(
  pid INTEGER REFERENCES Person(pid),   -- added
  hid   INTEGER,                       -- added
  did INTEGER,
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, hid, did),                    -- added
  FOREIGN KEY (pid) REFERENCES LegAnalystOffice(pid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Just a specific state agency.

  Source: CA: Transcription Tool (Some added by hand)
          NY: Transcription Tool (Some added by hand)
 */
CREATE TABLE IF NOT EXISTS StateAgency (
  name VARCHAR(200),
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  A person who works for a state agency.

  Source: CA: Transcription Tool
          NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS StateAgencyRep(
  pid INTEGER,   -- added
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid),                    -- added
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  FOREIGN KEY (employer, state) REFERENCES StateAgency(name, state)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  An instance of a state agency rep testifying at a hearing.

  Source: CA: Transcription Tool
          NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS StateAgencyRepRepresentation(
  pid INTEGER,   -- added
  employer VARCHAR(200),
  position VARCHAR(100),
  hid   INTEGER,                       -- added
  did INTEGER,
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, hid, did),                    -- added
  FOREIGN KEY (pid) REFERENCES StateAgencyRep(pid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  FOREIGN KEY (employer, state) REFERENCES StateAgency(name, state)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  State constitutional offices are important organizations in state legislatures.

  Source: CA: Transcription Tool
          NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS StateConstOffice (
  name VARCHAR(200),
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Table holds recorded of specific state const offices.

  Source: CA: Transcription Tool
          NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS StateConstOfficeRep(
  pid INTEGER,
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid),                    -- added
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Specific instance of a state const office employee testifying at a hearing.

  Source: CA: Transcription Tool
          NY: Transcription Tool
 */
CREATE TABLE IF NOT EXISTS StateConstOfficeRepRepresentation(
  pid INTEGER,
  office VARCHAR(200),
  position VARCHAR(200),
  hid INTEGER,
  did INTEGER,
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (pid, hid, did),                    -- added
  FOREIGN KEY (pid) REFERENCES StateConstOfficeRep(pid),
  FOREIGN KEY (hid) REFERENCES Hearing(hid),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  FOREIGN KEY (office, state) REFERENCES StateConstOffice(name, state)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/* Entity::Payors

   Payors are persons or organizations that pay for another organization
   on the "behest" of a legislator.

   Source: CA: refactored_insert_Behests.py
           NY: No Data
*/
CREATE TABLE IF NOT EXISTS Payors (
  prid INT AUTO_INCREMENT,  -- Payor id
  name VARCHAR(200),        -- name
  city VARCHAR(50),         -- city
  state VARCHAR(2),
  addressState VARCHAR(2),         -- U.S. state
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY(prid),
  FOREIGN KEY (addressState) REFERENCES State(abbrev),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/* Entity::Behests

   Behests are when a legislator asks someone (can be an Organization or a
   Person) to pay for another organization. Essentially, the legislator gets
   good publicity for helping the payee. Later on, the payor can influence
   the legislator on certain bills because they helped out before.

   Source: CA: refactored_insert_Behests.py
           NY: No Data
*/
CREATE TABLE IF NOT EXISTS Behests (
  official INT,          -- legislator (ref. Legislator.pid)
  datePaid DATE,         -- date the payor paid
  datePaid_ts INT(11) AS (UNIX_TIMESTAMP(datePaid)), -- Used by Drupal
  payor INT,             -- organization/person that paid (ref. Payors.pid)
  amount INT,            -- amount given to payee in USD
  payee INT,             -- organization that was paid (ref. Organizations.oid)
  description TEXT,      -- description of the exchange
  purpose VARCHAR(200),  -- purpose of behest (ex. Charitable)
  noticeReceieved DATE,  -- when the behest was filed
  noticeReceived_ts INT(11) AS (UNIX_TIMESTAMP(noticeReceived)), -- Used by Drupal
  sessionYear YEAR,
  state VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY(official, payor, payee, datePaid),
  FOREIGN KEY(official) REFERENCES Person(pid),
  FOREIGN KEY(payor) REFERENCES Payors(prid),
  FOREIGN KEY(payee) REFERENCES Organizations(oid),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


-- TODO are we still using this?
/*
  Table used by the front to keep track of specific bill types.

  Sources: CA: Manually
           NY: Manually
 */
CREATE TABLE IF NOT EXISTS BillTypes (
  Type VARCHAR(10),
  Label VARCHAR(10),
  House VARCHAR(100),
  State VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


-- TODO are we still using this?
/*
  Table used by the front to keep track of speaker profile types.

  Sources: CA: Manually
           NY: Manually
 */
CREATE TABLE IF NOT EXISTS SpeakerProfileTypes  (
  SpeakerType VARCHAR(50),
  Label VARCHAR(50),
  State VARCHAR(2),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


-- TODO this is probably deprecated
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
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY(analysis_id)
);

/*
  Holds gifts given to staff members of legislators or commitees.

  Sources: CA: ImportLegStaffGifts
           NY: No Data
 */
CREATE TABLE IF NOT EXISTS LegStaffGifts (
  year YEAR,
  session_year YEAR,
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
  date_given_ts INT(11) AS (UNIX_TIMESTAMP(date_given)), -- Used by Drupal
  gift_value DECIMAL,
  reimbursed BOOLEAN, -- this one is just a flag
  gift_description VARCHAR(200),
  speech_or_panel BOOLEAN, -- flag to see if was for a speech
  image_url VARCHAR(2000),
  schedule ENUM('D', 'E'),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  FOREIGN KEY (staff_member) REFERENCES Person(pid),
  FOREIGN KEY  (legislator) REFERENCES Legislator(pid)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Holds information on the employment of a legislative staff member by a specific legislator.

  Sources: CA: ImportLegStaffGifts
           NY: No Data
 */
CREATE TABLE IF NOT EXISTS LegOfficePersonnel (
  staff_member INT,
  legislator INT, -- pk for term
  term_year YEAR, -- pk for term
  house VARCHAR(100), -- pk for term
  start_date DATE, -- When the staff member started at this office
  start_date_ts INT(11) AS (UNIX_TIMESTAMP(start_date)), -- Used by Drupal
  end_date DATE,  -- when staff member ended with that office
  end_date_ts INT(11) AS (UNIX_TIMESTAMP(end_date)), -- Used by Drupal
  title VARCHAR(100),
  state CHAR(2), -- pk for term
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (staff_member, legislator, term_year, start_date),
  FOREIGN KEY (staff_member) REFERENCES LegislativeStaff(pid),
  FOREIGN KEY (legislator, term_year, house, state)
  REFERENCES Term(pid, year, house, state)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  A specific office within the state legislature. Somehow different from state const office.

  Sources: CA: ImportLegStaffGifts
           NY: No Data
 */
CREATE TABLE IF NOT EXISTS LegislatureOffice (
  lo_id INT AUTO_INCREMENT,
  name VARCHAR(200),
  house VARCHAR(200),
  state CHAR(2), -- pk for term
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

  PRIMARY KEY (lo_id),
  UNIQUE (name, house, state),
  FOREIGN KEY (house, state) REFERENCES House(name, state)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  The employment of a legislative staff member to a given office.

  Sources: CA: ImportLegStaffGifts
           NY: No Data
 */
CREATE TABLE IF NOT EXISTS OfficePersonnel (
  staff_member INT,
  office INT, -- pk for term
  start_date DATE, -- When the staff member started at this office
  end_date DATE,  -- when staff member ended with that office
  state CHAR(2), -- pk for term
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (staff_member, office, start_date),
  FOREIGN KEY (staff_member) REFERENCES LegislativeStaff(pid),
  FOREIGN KEY (office) REFERENCES LegislatureOffice(lo_id)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

/*
  Temporary table used to track organizations that should be grouped as one entity.
 */
CREATE TABLE OrgConcept (
  oid INT,
  name VARCHAR(250),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (oid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


/*
  Temporary table used to track organizations that should be grouped as one entity.
 */
CREATE TABLE OrgConceptAffiliation (
  new_oid INT,
  old_oid INT,
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

  PRIMARY KEY (new_oid, old_oid),
  FOREIGN KEY (new_oid) REFERENCES OrgConcept(oid),
  FOREIGN KEY (old_oid) REFERENCES Organizations(oid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


-- TODO this should no longer be necessary
/* Entity::DeprecatedPerson

   This is used for tracking what people are deprecated and will flush them
   out at a set time.

   Used by: Toshi
*/
CREATE TABLE IF NOT EXISTS DeprecatedPerson(
  pid INTEGER,     -- Person id (ref. Person.pid)
  dr_id INTEGER UNIQUE AUTO_INCREMENT,

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
  dr_id INTEGER UNIQUE AUTO_INCREMENT,
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

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
  status ENUM('downloading','downloaded','download failed','skipped','queued','cutting','cut','cutting failed','approved','tasked','deleted','tasking','tasking failed','archived'),
  hid INT(11),
  glacierId VARCHAR(255),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  PRIMARY KEY (videoId),
  FOREIGN KEY (hid) REFERENCES Hearing(hid)
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
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),

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
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  PRIMARY KEY (cutId),
  FOREIGN KEY (cutId) REFERENCES TT_Cuts(cutId)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS TT_HostingUrl (
  cutId INTEGER,
  url VARCHAR(255),
  streamUrl VARCHAR(255),
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (UNIX_TIMESTAMP(lastTouched)),
  PRIMARY KEY (cutId),
  FOREIGN KEY (cutId) REFERENCES TT_Cuts(cutId)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE OR REPLACE VIEW TT_currentCuts
AS SELECT * FROM TT_Cuts
WHERE current = TRUE AND finalized = FALSE ORDER BY videoId DESC, cutId ASC;
