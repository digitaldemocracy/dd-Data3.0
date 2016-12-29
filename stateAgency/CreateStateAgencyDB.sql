CREATE TABLE IF NOT EXISTS State (
  abbrev  VARCHAR(2),  -- eg CA, AZ
  country  VARCHAR(200), -- eg United States
  name   VARCHAR(200), -- eg Caliornia, Arizona
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(), 

  PRIMARY KEY (abbrev)
  )
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;


-- The specific state agency holding the hearing
CREATE TABLE IF NOT EXISTS StateAgency (
   sa_id INT AUTO_INCREMENT, -- pk for this datbase 
   name VARCHAR(255), -- the name of the state agency
   acronym VARCHAR(20), -- the acronym of the state agency
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (sa_id),
   UNIQUE (name, state),
   UNIQUE (acronym, state),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- Will include generic entries for Department Staff, Witness Testimony,
-- and Public Comment
CREATE TABLE IF NOT EXISTS Person (
   pid INTEGER AUTO_INCREMENT,   -- Person id
   last   VARCHAR(50) NOT NULL,     -- last name
   middle VARCHAR(50),              -- middle name
   first  VARCHAR(50) NOT NULL,     -- first name
   image VARCHAR(255),              -- path to image (if exists)
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(), 

   PRIMARY KEY (pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS servesOn (
   pid      INTEGER,                               -- Person id (ref. Person.pid)
   year     YEAR,                                  -- year served
   agency INTEGER,
   position ENUM('Boardmember', 'Executive Staff'),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (pid, year, agency, position),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (agency) REFERENCES StateAgency(sa_id)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Organization (
   oid INT AUTO_INCREMENT,
   name VARCHAR(255),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (oid),
   UNIQUE (name)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- Hearing
-- add fk to StateAgency because we have no CommitteeHearing
-- equivalent
-- include foreign key to agency
-- TODO a name?
CREATE TABLE IF NOT EXISTS Hearing (
   hid    INTEGER AUTO_INCREMENT,      -- Hearing id
   date   DATE,                        -- date of hearing
   agency INTEGER,
   state  VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (hid),
   UNIQUE (date, agency),
   FOREIGN KEY (agency) REFERENCES StateAgency(sa_id),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- same as DDDB2015Dec.Video
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

-- same as DDDB2015Dec.Video_ttml
CREATE TABLE IF NOT EXISTS Video_ttml (
   vid INTEGER,
   version INTEGER DEFAULT 0,
   ttml MEDIUMTEXT,
   source VARCHAR(4) DEFAULT 0,
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   FOREIGN KEY (vid) REFERENCES Video(vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS Document (
   doc_id INTEGER AUTO_INCREMENT,
   documentName VARCHAR(255),
   fileName VARCHAR(255),
   hid INTEGER,
   agency INTEGER,
   collection_date DATE,
   current BOOLEAN NOT NULL,
   url VARCHAR(255),
   sourceUrl VARCHAR(255),
   s3_url VARCHAR(255),
   state VARCHAR(2),
   doc_type enum('agenda','action','minutes','transcript','supplementary'),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (doc_id),
   UNIQUE KEY (fileName),
   UNIQUE KEY (url),
   FOREIGN KEY (agency) REFERENCES StateAgency(sa_id),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE OR REPLACE VIEW currentDocument 
AS SELECT doc_id, documentName, fileName, hid, agency, collection_date, url, 
  state, doc_type, lastTouched 
FROM Document 
WHERE current = TRUE ORDER BY collection_date DESC;

-- similar to DDDB2015.BillDiscussion but includes a name
-- * this is the one change Darius will need to make
CREATE TABLE IF NOT EXISTS AgendaItem (
   ai_id       INTEGER AUTO_INCREMENT,
   name        VARCHAR(255),
   position    VARCHAR(20),
   hid         INTEGER,
   startVideo  INTEGER,
   startTime   INTEGER,
   endVideo    INTEGER,
   endTime     INTEGER,
   numVideos   INTEGER(4),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (ai_id),
   UNIQUE KEY (name, startVideo, startTime),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (startVideo) REFERENCES Video(vid),
   FOREIGN KEY (endVideo) REFERENCES Video(vid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS represents (
   pid      INTEGER,
   organization INTEGER,
   agenda_item INTEGER,
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (pid, agenda_item),
   FOREIGN KEY (pid) REFERENCES Person(pid),
   FOREIGN KEY (organization) REFERENCES Organization(oid),
   FOREIGN KEY (agenda_item) REFERENCES AgendaItem(ai_id)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- specifies relationship between supplementary document
-- and its associated agenda item
CREATE TABLE IF NOT EXISTS AgendaDocument (
   doc_id INTEGER,
   hid INTEGER,
   ai_id INTEGER,

   PRIMARY KEY (doc_id, ai_id),
   FOREIGN KEY (doc_id) REFERENCES Document(doc_id),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (ai_id) REFERENCES AgendaItem(ai_id)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- specifies parent-child relationship between agenda items
CREATE TABLE IF NOT EXISTS AgendaHierarchy (
   hid INTEGER,
   parent INTEGER,
   child INTEGER,

   PRIMARY KEY (child),
   FOREIGN KEY (hid) REFERENCES Hearing(hid),
   FOREIGN KEY (parent) REFERENCES AgendaItem(ai_id),
   FOREIGN KEY (child) REFERENCES AgendaItem(ai_id)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- same as DDDB2015Dec.Utterance-ish
-- Dropped type and alignment
-- *pid->speaker, did->agenda_item so we're consistent across this db
CREATE TABLE IF NOT EXISTS Utterance (
   uid    INTEGER AUTO_INCREMENT,
   vid    INTEGER,
   speaker INTEGER,
   time   INTEGER,
   endTime INTEGER,
   text   TEXT,
   current BOOLEAN NOT NULL,
   finalized BOOLEAN NOT NULL,
   dataFlag INTEGER DEFAULT 0,
   diarizationTag VARCHAR(5) DEFAULT '',
   agenda_item INTEGER,
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (uid, current),
   UNIQUE KEY (uid, vid, speaker, current, time),
   FOREIGN KEY (speaker) REFERENCES Person(pid),
   FOREIGN KEY (vid) REFERENCES Video(vid),
   FOREIGN KEY (agenda_item) REFERENCES AgendaItem(ai_id),
   FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

-- currentUtterance
CREATE OR REPLACE VIEW currentUtterance 
AS SELECT uid, vid, speaker, time, endTime, text, state, agenda_item, 
  lastTouched 
FROM Utterance 
WHERE current = TRUE AND finalized = TRUE ORDER BY time DESC;

-- Added agency reference
-- Added sourceUrl, hid
-- Changed status
CREATE TABLE IF NOT EXISTS TT_Videos (
   videoId INTEGER AUTO_INCREMENT,
   hearingName VARCHAR(255),
   hearingDate DATE,
   agency INTEGER,
   url VARCHAR(255),
   fileName VARCHAR(255),
   duration FLOAT,
   sourceUrl VARCHAR(255),
   state VARCHAR(2),
   status ENUM('downloading','downloaded','download failed','skipped','queued','cut','cutting','cutting failed','approved','tasked','deleted','tasking','tasking failed','archived'),
   glacierId VARCHAR(255),
   hid INT(11),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   PRIMARY KEY (videoId),
   FOREIGN KEY (agency) REFERENCES StateAgency(sa_id),
   FOREIGN KEY (state) REFERENCES State(abbrev),
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
   streamUrl VARCHAR(255),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
   PRIMARY KEY (cutId),
   FOREIGN KEY (cutId) REFERENCES TT_Cuts(cutId)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE OR REPLACE VIEW TT_currentCuts 
AS SELECT * FROM TT_Cuts 
WHERE current = TRUE AND finalized = FALSE ORDER BY videoId DESC, cutId ASC;

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

-- same as DDDB2015Dec.TT_Task
CREATE TABLE IF NOT EXISTS TT_Task (
   tid INTEGER AUTO_INCREMENT ,
   hid INTEGER ,    -- added
   agenda_item INTEGER , 
   editor_id INTEGER ,
   name VARCHAR(1000) NOT NULL , 
   vid INTEGER , 
   startTime INTEGER NOT NULL , 
   endTime INTEGER NOT NULL , 
   created DATE,
   assigned DATE, 
   completed DATE,
   priority INTEGER,
   
   PRIMARY KEY (tid),
   FOREIGN KEY (agenda_item) REFERENCES AgendaItem(ai_id),
   FOREIGN KEY (editor_id) REFERENCES TT_Editor(id),
   FOREIGN KEY (vid) REFERENCES Video(vid),
   FOREIGN KEY (hid) REFERENCES Hearing(hid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS TT_EditorStates (
  tt_user INTEGER,
  state VARCHAR(2),
  priority INTEGER,

  PRIMARY KEY (tt_user, state),
  FOREIGN KEY (tt_user) REFERENCES TT_Editor(id),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/*  LEGACY TABLES  */
/* Entity::DeprecatedPerson

   This is used for tracking what people are deprecated and will flush them 
   out at a set time.

   Used by: Toshi
*/
CREATE TABLE IF NOT EXISTS DeprecatedPerson (
    pid INTEGER,     -- Person id (ref. Person.pid)
    
    PRIMARY KEY(pid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;

/* Entity::DeprecatedOrganizations

   This is used for tracking what Organizations are deprecated

   Used by: Toshi
*/
CREATE TABLE IF NOT EXISTS DeprecatedOrganization (
   oid INTEGER,      -- Organization id (ref. Organization.oid)
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY(oid)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;
