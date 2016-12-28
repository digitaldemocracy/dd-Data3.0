/*
  The subset of utterances that actually appear on the site.
  *Note that this is actually maintained as a regular view.
 */
CREATE OR REPLACE VIEW currentUtterance
AS SELECT uid, vid, pid, time, endTime, text, type, alignment, state, did,
     lastTouched, lastTouched_ts
   FROM Utterance
   WHERE current = TRUE AND finalized = TRUE ORDER BY time DESC;


/*
  Gives the participation for non-legislator speakers in transcribed Hearings
*/
CREATE TABLE SpeakerParticipation (
  pid INT,
  session_year YEAR,
  state VARCHAR(2),
  WordCountTotal FLOAT, -- Total words spoken
  WordCountHearingAvg FLOAT, -- Average words per Hearing
  TimeTotal INT, -- Total time based on length of utterances
  TimeHearingAvg FLOAT, -- Average time per Hearing
  dr_id INT,

  -- fk constraints not enforced, here for convenience
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (session_year, state) REFERENCES Session(start_year, state),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  UNIQUE (dr_id)
);

CREATE TABLE OrgAlignments (
  oa_id INT AUTO_INCREMENT,
  oid int(11) DEFAULT NULL,
  bid varchar(23) CHARACTER SET utf8 DEFAULT NULL,
  hid int(11) DEFAULT NULL,
  alignment char(20) CHARACTER SET utf8 DEFAULT NULL,
  analysis_flag BOOL,
  state VARCHAR(2),

  PRIMARY KEY(oa_id),
  UNIQUE (oid, bid, hid, alignment, analysis_flag),
  FOREIGN KEY (state) REFERENCES State(abbrev)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

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

-- Creates the alignments view toshi needs for his
-- query.
CREATE VIEW BillAlignments
AS
  SELECT MAX(u.uid) AS uid, l.pid, u.alignment, u.did
  FROM Lobbyist l
    JOIN currentUtterance u
      ON l.pid = u.pid
  WHERE u.did IS NOT NULL
  GROUP BY l.pid, u.alignment, u.did;


-- Used by Kristian to affiliate people to their proper labels
CREATE TABLE IF NOT EXISTS `PersonAffiliations` (
  `pid` int(11) DEFAULT NULL,
  `affiliation` varchar(255) DEFAULT NULL,
  `state` varchar(2) DEFAULT NULL,
  `source` varchar(255) DEFAULT NULL,
  `dr_id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`dr_id`),
  KEY `state` (`state`),
  KEY `pid_state` (`pid`,`state`),
  KEY `affiliation` (`affiliation`),
  KEY `pid` (`pid`)
) ENGINE=INNODB DEFAULT CHARSET=latin1;

-- Used by Kristian for Drupal. Rebuilt by a mysql event every night
-- WRONG COLUMNS
CREATE TABLE IF NOT EXISTS `PersonClassifications` (
  `pid` int(11) DEFAULT NULL,
  `classification` varchar(255) DEFAULT NULL,
  `state` varchar(2) DEFAULT NULL,
  `dr_id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`dr_id`),
  KEY `pid` (`pid`),
  KEY `pid_state` (`pid`,`state`),
  KEY `state` (`state`),
  KEY `classification` (`classification`)
) ENGINE=INNODB DEFAULT CHARSET=latin1;

-- Used by Kristian to determine the first utterance spoken by a speaker at given
-- BillDiscussion
CREATE TABLE IF NOT EXISTS InitialUtterance (
  pid INT,
  uid INT,
  did INT,

  PRIMARY KEY (pid, uid, did),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (uid) REFERENCES Utterance(uid),
  FOREIGN KEY (did) REFERENCES BillDiscussion(did)
);


-- A combination of Gift and LegStaffGifts. Rebuilds each night from a mysql event
CREATE TABLE `GiftCombined` (
  `RecordId` int(11) NOT NULL AUTO_INCREMENT,
  `recipientPid` int(11) DEFAULT NULL,
  `legislatorPid` int(11) DEFAULT NULL,
  `giftDate` date DEFAULT NULL,
  `giftDate_ts` int(11) DEFAULT NULL,
  `year` year(4) DEFAULT NULL,
  `description` varchar(150) DEFAULT NULL,
  `giftValue` double DEFAULT NULL,
  `agencyName` varchar(100) DEFAULT NULL,
  `sourceName` varchar(150) DEFAULT NULL,
  `sourceBusiness` varchar(100) DEFAULT NULL,
  `sourceCity` varchar(50) DEFAULT NULL,
  `sourceState` varchar(30) DEFAULT NULL,
  `imageUrl` varchar(200) DEFAULT NULL,
  `oid` int(11) DEFAULT NULL,
  `activity` varchar(256) DEFAULT NULL,
  `position` varchar(200) DEFAULT NULL,
  `schedule` enum('D','E') DEFAULT NULL,
  `jurisdiction` varchar(200) DEFAULT NULL,
  `distictNumber` int(11) DEFAULT NULL,
  `reimbursed` tinyint(1) DEFAULT NULL,
  `giftIncomeFlag` tinyint(1) DEFAULT '0',
  `speechFlag` tinyint(1) DEFAULT '0',
  `speechOrPanel` tinyint(1) DEFAULT NULL,
  `state` varchar(2) DEFAULT NULL,
  `lastTouched` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`RecordId`),
  KEY `giftDate_ts` (`giftDate_ts`),
  KEY `recipientPid` (`recipientPid`),
  KEY `legislatorPid` (`legislatorPid`),
  KEY `agencyName` (`agencyName`),
  KEY `sourceName` (`sourceName`),
  KEY `giftValue` (`giftValue`),
  KEY `state` (`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

# Basically like current utterance. Rebuilt every night
CREATE TABLE IF NOT EXISTS BillVersionCurrent (
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


CREATE TABLE UnionedRepresentations (
  pid INT,
  hid INT,
  did INT,
  oid INT,
  state VARCHAR(2)
);

CREATE TABLE UnionedLobbyistEmployers (
  pid INT,
  assoc_name VARCHAR(255),
  rpt_date DATE,
  rpt_date_ts INT,
  beg_year YEAR,
  end_year YEAR,
  state VARCHAR(2)
);

CREATE TABLE KnownClients (
  pid INT,
  assoc_name VARCHAR(255),
  oid INT,
  state VARCHAR(2)
);

CREATE TABLE BillAlignmentScoresMiguel (
  aligned_votes int,
  alignment_percentage double,
  bid varchar(63),
  oid int,
  pid int,
  total_votes int,

  PRIMARY KEY (bid, oid, pid)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE BillAlignmentScoresAndrew (
  bid varchar(63),
  oid int,
  pid int,
  aligned_votes int,
  alignment_percentage double,
  total_votes int,
  positions int,
  affirmations int,

  PRIMARY KEY (bid, oid, pid)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE AlignmentScores (
  pid int,
  oid int,
  MiguelScore double,
  AndrewScore double,

  PRIMARY KEY (pid, oid)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE AlignmentScoresData (
  bill VARCHAR(30),
  leg_first VARCHAR(30),
  leg_last VARCHAR(30),
  pid INT,
  leg_alignment ENUM('For', 'Against'),
  leg_vote_date DATE,
  org_name VARCHAR(200),
  oid INT,
  org_alignment ENUM('For', 'Against'),
  date_of_org_alignment DATE,

  FOREIGN KEY (bill) REFERENCES Bill(bid),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (oid) REFERENCES OrgConcept(oid)

) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE AlignmentScoresExtraInfo (
  oid INT,
  pid INT,
  positions_registered INT,
  votes_in_agreement INT,
  votes_in_disagreement INT,
  affirmations INT,
  bills INT,

  PRIMARY KEY (oid, pid),
  FOREIGN KEY (oid) REFERENCES OrgConcept(oid),
  FOREIGN KEY (pid) REFERENCES Person(pid)

) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE AlignmentScoresAggregated (
  oid INT,
  house VARCHAR(100),
  party ENUM('Republican', 'Democrat', 'Other'),
  state VARCHAR(2),
  score DOUBLE,
  positions_registered INT,
  votes_in_agreement INT,
  votes_in_disagreement INT,
  affirmations INT,
  bills INT,

  PRIMARY KEY (oid, house, party, state),
  FOREIGN KEY (oid) REFERENCES OrgConcept(oid),
  FOREIGN KEY (house, state) REFERENCES House(name, state)

) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE OR REPLACE VIEW CombinedAlignmentScores
AS
  SELECT a.pid,
    a.oid,
    null as house,
    null as party,
    'CA' as 'state',
    a.AndrewScore as score,
    asei.positions_registered,
    asei.votes_in_agreement,
    asei.votes_in_disagreement
  FROM AlignmentScores a
    INNER JOIN AlignmentScoresExtraInfo asei
      ON asei.oid = a.oid AND asei.pid = a.pid
  UNION
  SELECT null,
    asa.oid,
    asa.house,
    asa.party,
    asa.state,
    asa.score,
    asa.positions_registered,
    asa.votes_in_agreement,
    asa.votes_in_disagreement
  FROM AlignmentScoresAggregated asa;
