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
   state VARCHAR(2),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (sa_id),
   UNIQUE (name, state)
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
   agency INT,
   position ENUM('Boardmember', 'Executive Staff'),
   lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

   PRIMARY KEY (pid, year, agency, position),
   FOREIGN KEY (agency) REFERENCES StateAgency(sa_id)
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
   agency INT,
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


-- similar to DDDB2015.BillDiscussion but includes a name
-- * this is the one change Darius will need to make
CREATE TABLE IF NOT EXISTS AgendaItem (
   ai_id       INTEGER AUTO_INCREMENT,
   name        VARCHAR(255),
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
   agenda_item INT,
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
   priority INT,
   
   PRIMARY KEY (tid),
   FOREIGN KEY (agenda_item) REFERENCES AgendaItem(ai_id),
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



