
DROP EVENT IF EXISTS PersonAffiliations_event;
delimiter |

CREATE EVENT PersonAffiliations_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2016-07-16 07:15:00'
  DO
  BEGIN

    SELECT 'Building PersonClassifications Table...' AS '';
    DROP TABLE IF EXISTS `PersonClassifications`;

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
    ) ENGINE=MyISAM DEFAULT CHARSET=latin1;

    TRUNCATE TABLE PersonClassifications;

    INSERT INTO PersonClassifications (pid, state, classification) select `gp`.`pid` AS `pid`,`gp`.`state` AS `state`,'General Public' AS `classification` from `GeneralPublic` `gp` union select `lao`.`pid` AS `pid`,`lao`.`state` AS `state`,'Legislative Analyst Office' AS `classification` from `LegAnalystOffice` `lao` union select `ls`.`pid` AS `pid`,`ls`.`state` AS `state`,'Legislative Staff' AS `classification` from `LegislativeStaff` `ls` union select `lsr`.`pid` AS `pid`,`lsr`.`state` AS `state`,'Legislative Staff Representation' AS `classification` from `LegislativeStaffRepresentation` `lsr` union select `l`.`pid` AS `pid`,`l`.`state` AS `state`,'Legislator' AS `classification` from `Legislator` `l` union select `lob`.`pid` AS `pid`,`lob`.`state` AS `state`,'Lobbyist' AS `classification` from `Lobbyist` `lob` union select `lobr`.`pid` AS `pid`,`lobr`.`state` AS `state`,'Lobbyist Representation' AS `classification` from `LobbyistRepresentation` `lobr` union select `sar`.`pid` AS `pid`,`sar`.`state` AS `state`,'State Agency Rep' AS `classification` from `StateAgencyRep` `sar` union select `sarr`.`pid` AS `pid`,`sarr`.`state` AS `state`,'State Agency Rep Representative' AS `classification` from `StateAgencyRepRepresentation` `sarr` union select `scor`.`pid` AS `pid`,`scor`.`state` AS `state`,'State Constitutional Office Rep' AS `classification` from `StateConstOfficeRep` `scor` union select `scorr`.`pid` AS `pid`,`scorr`.`state` AS `state`,'State Constitutional Office Rep Representative' AS `classification` from `StateConstOfficeRepRepresentation` `scorr`;

  END |

delimiter ;


DROP EVENT IF EXISTS BillVersionCurrent_event;
delimiter |

CREATE EVENT BillVersionCurrent_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2016-07-16 07:15:00'
DO
  BEGIN

    SELECT 'Creating BillVersionCurrent table...' AS '';
    DROP TABLE IF EXISTS BillVersionCurrent;
    CREATE TABLE BillVersionCurrent LIKE BillVersion;
    INSERT INTO BillVersionCurrent
    (vid, bid, date, billState, subject, appropriation, substantive_changes, title, digest, text, state)
      SELECT vid, bid, date, billState, subject, appropriation, substantive_changes, title, digest, text, state
      FROM
        (SELECT *
         FROM BillVersion bv
         ORDER BY bv.date DESC, bv.vid ASC) bvc
      GROUP BY bvc.bid;

  END |

delimiter ;


DROP EVENT IF EXISTS GiftCombined_event;
delimiter |

CREATE EVENT GiftCombined_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2016-07-16 07:15:00'
DO
  BEGIN

    SELECT 'Creating GiftCombined Table...' AS '';
    DROP TABLE IF EXISTS `GiftCombined`;
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
      `districtNumber` int(11) DEFAULT NULL,
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

    TRUNCATE TABLE GiftCombined;
    INSERT INTO GiftCombined (RecordId, recipientPid, schedule, sourceName, activity, sourceCity, sourceState, giftValue, giftDate, reimbursed, giftIncomeFlag, speechFlag, description, state, lastTouched, oid, giftDate_ts) SELECT * from Gift;
    INSERT INTO GiftCombined (year, agencyName, recipientPid, legislatorPid, position, districtNumber, jurisdiction, sourceName, sourceCity, sourceState, sourceBusiness, giftDate, giftValue, reimbursed, description, speechOrPanel, imageUrl, lastTouched, schedule, giftDate_ts) select year, agency_name, staff_member, legislator, position, district_number, jurisdiction, source_name, source_city, source_state, source_business, date_given, gift_value, reimbursed, gift_description, speech_or_panel, image_url, lastTouched, schedule, date_given_ts from LegStaffGifts;
    -- @TODO FIX LegStaffGifts - Temporary set state to CA for LegStaffGifts, since it doesn't contain a state yet
    UPDATE GiftCombined set state='CA' where state IS NULL;

  END |

delimiter ;

DROP EVENT IF EXISTS LegParticipation;
delimiter |

CREATE EVENT LegParticipation
  ON SCHEDULE
    EVERY 1 DAY STARTS '2016-07-16 07:00:00'
DO
  BEGIN

    DROP VIEW IF EXISTS UtterInfo;
    DROP VIEW IF EXISTS LegDidWord;
    DROP VIEW IF EXISTS DidWord;
    DROP VIEW IF EXISTS LegDidWordPer;
    DROP VIEW IF EXISTS LegHidWord;
    DROP VIEW IF EXISTS HearingWord;
    DROP VIEW IF EXISTS LegHidWordPer;
    DROP VIEW IF EXISTS LegWordAvg;
    DROP VIEW IF EXISTS TalkingLegs;
    DROP VIEW IF EXISTS ProperLegs;
    DROP VIEW IF EXISTS AllLegs;
    DROP VIEW IF EXISTS AllLegsInfo;
    DROP TABLE IF EXISTS LegParticipation;
    DROP VIEW IF EXISTS LegHearingParticipation;
    DROP TABLE IF EXISTS LegAvgPercentParticipation;

    -- Creates view with the word counts for every utterance
    CREATE VIEW UtterInfo
    AS
      SELECT u.uid, u.pid, v.hid, u.did, GetWordCount(u.text)
        AS WordCount, u.endTime - u.time AS Time
      FROM currentUtterance u
        JOIN Video v
          ON u.vid = v.vid
        JOIN Legislator l
          ON l.pid = u.pid;

    -- Gives the word counts for each legislator based on a
    -- hearing. Also their percentages talking.
    CREATE VIEW LegDidWord
    AS
      SELECT u.did, u.pid, SUM(u.WordCount) AS
        WordCount, SUM(u.Time) AS Time
      FROM UtterInfo u
      WHERE u.did IS NOT NULL
      GROUP BY u.pid, u.did;

    -- Gives the word count for a bill discussion
    -- using only legislators
    CREATE VIEW DidWord
    AS
      SELECT did, SUM(WordCount) AS WordCount, SUM(Time)
                                 AS Time
      FROM LegDidWord
      WHERE did IS NOT NULL
      GROUP BY did;

    -- give the percentage talking as well
    CREATE VIEW LegDidWordPer
    AS
      SELECT ldw.did, ldw.pid, ldw.WordCount, ldw.Time,
        ldw.WordCount / dw.WordCount AS PercentCount,
        ldw.Time / dw.Time AS PercentTime
      FROM LegDidWord ldw
        JOIN DidWord dw
          ON ldw.did = dw.did;


    -- Gets the word count for every legislator
    -- at a hearing
    CREATE VIEW LegHidWord
    AS
      SELECT u.pid, u.hid, SUM(u.WordCount) AS WordCount,
                           SUM(u.Time) AS Time
      FROM UtterInfo u
      GROUP BY u.pid, u.hid;

    -- Gets the word count for the hearing given only
    -- the legislators
    CREATE VIEW HearingWord
    AS
      SELECT hid, SUM(WordCount) AS WordCount,
                  SUM(Time) AS Time
      FROM LegHidWord
      GROUP BY hid;

    -- gives the percentage as well
    CREATE VIEW LegHidWordPer
    AS
      SELECT lhw.hid, lhw.pid, lhw.WordCount, lhw.Time,
        lhw.WordCount / hw.WordCount AS PercentCount,
        lhw.Time / hw.Time AS PercentTime
      FROM LegHidWord lhw
        JOIN HearingWord hw
          ON lhw.hid = hw.hid;

    -- Gets the average number of words a legislator speaks
    -- at a hearing
    CREATE VIEW LegWordAvg
    AS
      SELECT pid, SUM(WordCount) / COUNT(*) AS AvgWords
      FROM LegHidWord
      GROUP BY pid;

    -- All the legislators that should be a specific hearings
    CREATE VIEW ProperLegs
    AS
      SELECT DISTINCT ch.hid, s.pid
      FROM CommitteeHearings ch
        JOIN servesOn s
          ON s.cid = ch.cid;

    -- All the legislators that talked at specific hearings
    CREATE VIEW TalkingLegs
    AS
      SELECT DISTINCT hid, pid
      FROM UtterInfo;

    -- Combines the prior two tables to get all the legs at a
    -- hearing, as well as all the legs that should have been
    -- there
    CREATE VIEW AllLegs
    AS
      (SELECT hid, pid
       FROM ProperLegs)
      UNION
      (SELECT hid, pid
       FROM TalkingLegs);

    -- Collects everybody who should be present at a given
    -- did
    CREATE VIEW AllLegsInfo
    AS
      SELECT DISTINCT al.hid, bd.did, bd.bid, al.pid, p.first, p.last,
        t.party
      FROM AllLegs al
        LEFT JOIN BillDiscussion bd
          ON bd.hid = al.hid
        JOIN Person p
          ON al.pid = p.pid
        JOIN Term t
          ON p.pid = t.pid;

    -- Creates the necessary view for Leg participation
    CREATE TABLE LegParticipation
    AS
      SELECT al.hid, al.did, al.bid, al.pid, al.first, al.last,
        al.party,
        IFNULL(ldw.WordCount, 0) AS LegBillWordCount,
        IFNULL(ldw.Time, 0) AS LegBillTime,
        IFNULL(ldw.PercentCount, 0) AS LegBillPercentWord,
        IFNULL(ldw.PercentTime, 0) AS LegBillPercentTime,
        IFNULL(lhw.WordCount, 0) AS LegHearingWordCount,
        IFNULL(lhw.Time, 0) AS LegHearingTime,
        IFNULL(lhw.PercentCount, 0) AS LegHearingPercentWord,
        IFNULL(lhw.PercentTime, 0) AS LegHearingPercentTime,
        IFNULL(lwa.AvgWords, 0) AS LegHearingAvg,
        IFNULL(bdw.WordCount, 0) AS BillWordCount,
        IFNULL(hw.WordCount, 0) AS HearingWordCount
      FROM AllLegsInfo al
        LEFT JOIN LegDidWordPer ldw
          ON al.pid = ldw.pid
             AND al.did = ldw.did
        LEFT JOIN LegHidWordPer lhw
          ON al.pid = lhw.pid
             AND al.hid = lhw.hid
        LEFT JOIN LegWordAvg lwa
          ON al.pid = lwa.pid
        LEFT JOIN DidWord bdw
          ON al.did = bdw.did
        LEFT JOIN HearingWord hw
          ON al.hid = hw.hid;

    ALTER TABLE LegParticipation ADD COLUMN dr_id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY;

    -- Used to help you get the Avg Participation
    CREATE VIEW LegHearingParticipation
    AS
      SELECT DISTINCT pid, hid, first, last, LegHearingWordCount,
        HearingWordCount
      FROM LegParticipation;


    -- Gets the average percent participation for each leg
    CREATE TABLE LegAvgPercentParticipation
    AS
      SELECT pid, first, last, SUM(LegHearingWordCount) /
                               SUM(HearingWordCount) AS AvgPercentParticipation
      FROM LegParticipation
      GROUP BY pid;

    DROP VIEW UtterInfo;
    DROP VIEW LegDidWord;
    DROP VIEW DidWord;
    DROP VIEW LegDidWordPer;
    DROP VIEW LegHidWord;
    DROP VIEW HearingWord;
    DROP VIEW LegHidWordPer;
    DROP VIEW LegWordAvg;
    DROP VIEW TalkingLegs;
    DROP VIEW ProperLegs;
    DROP VIEW AllLegs;
    DROP VIEW AllLegsInfo;
    DROP VIEW LegHearingParticipation;

  END |

delimiter ;


DROP EVENT IF EXISTS OrgAlignments;
delimiter |

CREATE EVENT OrgAlignments
  ON SCHEDULE
    EVERY 1 DAY STARTS '2016-07-16 07:10:00'
DO
  BEGIN

    DROP VIEW IF EXISTS AllProfs;
    DROP VIEW IF EXISTS OrgAlignmentsUtter;
    DROP VIEW IF EXISTS OrgAlignmentsDistinct;
    DROP VIEW IF EXISTS OrgAlignmentsDefin;
    DROP VIEW IF EXISTS OrgAlignmentsTrumped;
    DROP VIEW IF EXISTS OrgAlignmentsUnknown;
    DROP VIEW IF EXISTS OrgAlignmentsMulti;
    DROP VIEW IF EXISTS OrgAMUtter;
    DROP VIEW IF EXISTS OrgAlignmentsExtra;

    -- Gives you the alignments of organizaitons on bills,
    -- based on the alignments of the lobbyists supporting them
    CREATE VIEW AllProfs
    AS
      SELECT pid, oid, did
      FROM LobbyistRepresentation
      UNION
      SELECT pid, oid, did
      FROM GeneralPublic;

    -- Binds utterances to every organization for each bill.
    -- It's set up so that it is almost like the organizations
    -- themselves said these things
    CREATE VIEW OrgAlignmentsUtter
    AS
      SELECT ap.oid, bd.bid, u.uid, u.alignment, bd.hid
      FROM AllProfs ap
        JOIN currentUtterance u
          ON ap.pid = u.pid
             AND ap.did = u.did
        JOIN BillDiscussion bd
          ON u.did = bd.did
      WHERE ap.oid IS NOT NULL;

    -- Uses group by to get all all the distinct alignments of
    -- an organization associated with the specific bill, at a
    -- specific hearing
    CREATE VIEW OrgAlignmentsDistinct
    AS
      SELECT oid, bid, alignment, hid
      FROM OrgAlignmentsUtter
      GROUP BY oid, bid, alignment, hid;

    -- Gets all the alignments we are certain about
    CREATE VIEW OrgAlignmentsDefin
    AS
      SELECT oid, bid, alignment, hid
      FROM OrgAlignmentsDistinct
      WHERE alignment != 'NA'
      GROUP BY oid, bid, hid
      HAVING COUNT(*) = 1;

    -- Next gets all cases where the indeterminate is
    -- trumped by a clear position
    CREATE VIEW OrgAlignmentsTrumped
    AS
      SELECT oid, bid, alignment, hid
      FROM OrgAlignmentsDistinct
      WHERE (oid, bid, hid) IN (SELECT oid, bid, hid
                                FROM OrgAlignmentsDistinct
                                GROUP BY oid, bid, hid
                                HAVING COUNT(*) = 2)
            AND alignment != 'Indeterminate'
            AND alignment != 'Neutral';

    -- Gets all the profiles that contain both for
    -- and against. These are left unknown
    CREATE VIEW OrgAlignmentsUnknown
    AS
      SELECT oid, bid, 'Unknown' AS alignment, hid
      FROM OrgAlignmentsDistinct
      WHERE (oid, bid, hid) IN (SELECT oid, bid, hid
                                FROM OrgAlignmentsDistinct
                                WHERE alignment = 'Against')
            AND alignment = 'For';

    -- Rounds up all the left over combos with multiple
    -- alignments
    CREATE VIEW OrgAlignmentsMulti
    AS
      SELECT oid, bid, hid
      FROM OrgAlignmentsDistinct
      WHERE (oid, bid, hid) IN (SELECT oid, bid, hid
                                FROM OrgAlignmentsDistinct
                                GROUP BY oid, bid, hid
                                HAVING COUNT(*) > 1)
            AND (oid, bid, hid) NOT IN (SELECT oid, bid, hid
                                        FROM OrgAlignmentsTrumped
                                        UNION
                                        SELECT oid, bid, hid
                                        FROM OrgAlignmentsUnknown);

    -- Binds the multi alignments to their utterances. You
    -- grab the latest valued one. Indeterminates are ignored
    CREATE VIEW OrgAMUtter
    AS
      SELECT oid, bid, hid, MAX(uid) AS uid
      FROM OrgAlignmentsUtter
      WHERE (oid, bid, hid) IN (SELECT oid, bid, hid
                                FROM OrgAlignmentsMulti
                                WHERE alignment != "Indeterminate")
      GROUP BY oid, bid, hid;

    -- Gets the alignment of that highest utterance
    CREATE VIEW OrgAlignmentsExtra
    AS
      SELECT oamu.oid, oamu.bid, oamu.hid, u.alignment
      FROM OrgAMUtter oamu
        JOIN currentUtterance u
          ON oamu.uid = u.uid;

    -- Gets all the alignments neatly into a table. This is what
    -- Toshi sees
    INSERT INTO OrgAlignments
    (oid, bid, hid, alignment, analysis_flag)
    SELECT t.*, 0
    FROM
      (SELECT oid, bid, hid, alignment
      FROM OrgAlignmentsDefin
      UNION
      SELECT oid, bid, hid, alignment
      FROM OrgAlignmentsTrumped
      UNION
      SELECT oid, bid, hid, alignment
      FROM OrgAlignmentsExtra) t
    WHERE (t.oid, t.bid, t.hid, t.alignment) NOT IN (SELECT oid, bid, hid, alignment
                                                     FROM OrgAlignments
                                                     WHERE analysis_flag = 0);

    DROP VIEW IF EXISTS AllProfs;
    DROP VIEW IF EXISTS OrgAlignmentsUtter;
    DROP VIEW IF EXISTS OrgAlignmentsDistinct;
    DROP VIEW IF EXISTS OrgAlignmentsDefin;
    DROP VIEW IF EXISTS OrgAlignmentsTrumped;
    DROP VIEW IF EXISTS OrgAlignmentsUnknown;
    DROP VIEW IF EXISTS OrgAlignmentsMulti;
    DROP VIEW IF EXISTS OrgAMUtter;
    DROP VIEW IF EXISTS OrgAlignmentsExtra;

  END |

delimiter ;
