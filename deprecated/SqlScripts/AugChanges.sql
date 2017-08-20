# Script stuff for modifying db
ALTER TABLE LobbyistEmployer
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (oid, state);

create table FoundSenderIds
as
  select *
  from LobbyingContracts
  where sender_id in (select filer_id
                      from LobbyistEmployer);

delete from LobbyingContracts
where sender_id not in (select sender_id
                        from FoundSenderIds);

drop table FoundSenderIds;

alter table LobbyingContracts
  add lobbyist_employer INT;

update LobbyingContracts lc, LobbyistEmployer le
set lc.lobbyist_employer = le.oid
where lc.sender_id = le.filer_id;

alter table LobbyingContracts
  drop FOREIGN KEY LobbyingContracts_ibfk_2,
  add FOREIGN KEY (lobbyist_employer, state) REFERENCES LobbyistEmployer(oid, state),
  drop primary key,
  add primary key (filer_id, lobbyist_employer, rpt_date, state),
  drop sender_id;

create table FoundSenderIds
as
select *
from LobbyistDirectEmployment
where sender_id in (select filer_id
                  from LobbyistEmployer);

delete from LobbyistDirectEmployment
where sender_id not in (select sender_id
                        from FoundSenderIds);

drop table FoundSenderIds;

alter table LobbyistDirectEmployment
  add lobbyist_employer INT;

update LobbyistDirectEmployment lde, LobbyistEmployer le
set lde.lobbyist_employer = le.oid
where lde.sender_id = le.filer_id;

alter table LobbyistDirectEmployment
  drop FOREIGN KEY LobbyistDirectEmployment_ibfk_2,
  add FOREIGN KEY (lobbyist_employer, state) REFERENCES LobbyistEmployer(oid, state),
  drop primary key,
  add primary key (pid, lobbyist_employer, rpt_date, ls_end_yr, state),
  drop sender_id;

alter table LobbyistRepresentation
    drop FOREIGN KEY LobbyistRepresentation_ibfk_2,
    add foreign key (oid, state) references LobbyistEmployer(oid, state);

# end script stuff for modifying db


-- Changes for Org Alignments
alter table OrgAlignments
    add analysis_flag boolean default False, 
	add oa_id INT auto_increment primary key;


alter table OrgAlignments
    add Unique(oid, bid, hid, alignment, analysis_flag);
	

-- Added for the transcription tool
# alter table TT_Videos
#   add hid int REFERENCES Hearing(hid);


# The following comes from Kristian
SET sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';

-- PersonAffiliations Table
SELECT 'Building PersonAffiliations Table...' AS '';

DROP TABLE IF EXISTS `PersonAffiliations`;
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
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

TRUNCATE TABLE PersonAffiliations;
INSERT INTO PersonAffiliations (pid, affiliation, state, source)
  select `gp`.`pid` AS `pid`,
         `o`.`name` AS `affiliation`,
         `gp`.`state` AS `state`,
         'General Public' AS `source`
  from (`GeneralPublic` `gp`
    join `Organizations` `o`
    on((`gp`.`oid` = `o`.`oid`)))
  union
  select `lde`.`pid` AS `pid`,
         `o`.`name` AS `affiliation`,
         `lde`.`state` AS `state`,
         'Lobbyist' AS `source`
  from ((`LobbyistDirectEmployment` `lde`
    join `LobbyistEmployer` `le`
    on((`le`.`oid` = `lde`.`lobbyist_employer`)))
    join `Organizations` `o`
    on((`le`.`oid` = `o`.`oid`)))
  union
  select `lr`.`pid` AS `pid`,
          `o`.`name` AS `affiliation`,
          `lr`.`state` AS `state`,
          'Lobbyist' AS `source`
   from (`LobbyistRepresentation` `lr`
     join `Organizations` `o`
       on((`lr`.`oid` = `o`.`oid`)))
  union
  select `laor`.`pid` AS `pid`,
         `o`.`name` AS `affiliation`,
         `laor`.`state` AS `state`,
         'Legislative Analyst' AS `source`
  from ((`LegAnalystOfficeRepresentation` `laor`
    join `OrgAlignments` `oa`
    on((`oa`.`hid` = `laor`.`hid`)))
    join `Organizations` `o`
    on((`oa`.`oid` = `o`.`oid`)))
  union
  select `lsr`.`pid` AS `pid`,
         `c`.`name` AS `affiliation`,
         `lsr`.`state` AS `state`,
         'Legislative Staff' AS `source`
  from (`LegislativeStaffRepresentation` `lsr`
    join `Committee` `c`
    on((`c`.`cid` = `lsr`.`committee`)))
  union
  select `sarr`.`pid` AS `pid`,
         `sarr`.`employer` AS `affiliation`,
         `sarr`.`state` AS `state`,
         'State Agency' AS `source`
  from `StateAgencyRepRepresentation` `sarr`
  union
  select `scorr`.`pid` AS `pid`,
         `scorr`.`office` AS `affiliation`,
         `scorr`.`state` AS `state`,
         'State Constitutional Office' AS `source`
  from `StateConstOfficeRepRepresentation` `scorr`;

-- PersonClassifications Table
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


-- Date Timestamp Table Modifications
SELECT 'Adding *_ts date columns to tables...' AS '';
ALTER TABLE Action ADD COLUMN date_ts int(11) AS (TO_SECONDS(date) - TO_SECONDS('1970-01-01'));
ALTER TABLE Behests ADD COLUMN datePaid_ts int(11) AS (TO_SECONDS(datePaid) - TO_SECONDS('1970-01-01'));
ALTER TABLE Behests ADD COLUMN noticeReceived_ts int(11) AS (TO_SECONDS(noticeReceived) - TO_SECONDS('1970-01-01'));
ALTER TABLE BillVersion ADD COLUMN date_ts int(11) AS (TO_SECONDS(date) - TO_SECONDS('1970-01-01'));
ALTER TABLE BillVoteSummary ADD COLUMN VoteDate_ts int(11) AS (TO_SECONDS(VoteDate) - TO_SECONDS('1970-01-01'));
ALTER TABLE Contribution ADD COLUMN date_ts int(11) AS (TO_SECONDS(date) - TO_SECONDS('1970-01-01'));
ALTER TABLE Gift ADD COLUMN giftDate_ts int(11) AS (TO_SECONDS(giftDate) - TO_SECONDS('1970-01-01'));
ALTER TABLE Hearing ADD COLUMN date_ts int(11) AS (TO_SECONDS(date) - TO_SECONDS('1970-01-01'));
ALTER TABLE HearingAgenda ADD COLUMN date_created_ts int(11) AS (TO_SECONDS(date_created) - TO_SECONDS('1970-01-01'));
ALTER TABLE LegOfficePersonnel ADD COLUMN start_date_ts int(11) AS (TO_SECONDS(start_date) - TO_SECONDS('1970-01-01'));
ALTER TABLE LegOfficePersonnel ADD COLUMN end_date_ts int(11) AS (TO_SECONDS(end_date) - TO_SECONDS('1970-01-01'));
ALTER TABLE LegStaffGifts ADD COLUMN date_given_ts int(11) AS (TO_SECONDS(date_given) - TO_SECONDS('1970-01-01'));
ALTER TABLE LobbyingContracts ADD COLUMN rpt_date_ts int(11) AS (TO_SECONDS(rpt_date) - TO_SECONDS('1970-01-01'));
ALTER TABLE LobbyingFirmState ADD COLUMN rpt_date_ts int(11) AS (TO_SECONDS(rpt_date) - TO_SECONDS('1970-01-01'));
ALTER TABLE LobbyistDirectEmployment ADD COLUMN rpt_date_ts int(11) AS (TO_SECONDS(rpt_date) - TO_SECONDS('1970-01-01'));
ALTER TABLE LobbyistEmployment ADD COLUMN rpt_date_ts int(11) AS (TO_SECONDS(rpt_date) - TO_SECONDS('1970-01-01'));
ALTER TABLE LobbyistRepresentation ADD COLUMN hearing_date_ts int(11) AS (TO_SECONDS(hearing_date) - TO_SECONDS('1970-01-01'));
ALTER TABLE Term ADD COLUMN start_ts int(11) AS (TO_SECONDS(start) - TO_SECONDS('1970-01-01'));
ALTER TABLE Term ADD COLUMN end_ts int(11) AS (TO_SECONDS(end) - TO_SECONDS('1970-01-01'));

# Clean some bad data
DELETE FROM Contribution
WHERE year(date) > 2020;

DELETE FROM Gift
WHERE year(giftDate) > 2020
      or year(giftDate) < 1950;

DELETE FROM LobbyingContracts
WHERE year(rpt_date) > 2020
      or year(rpt_date) < 1950;

DELETE FROM LobbyistEmployment
WHERE year(rpt_date) > 2020
      or year(rpt_date) < 1950;

DELETE FROM LobbyistEmployment
WHERE sender_id = '1282555';

DELETE FROM LobbyingFirmState
WHERE year(rpt_date) > 2020
      or year(rpt_date) < 1950;

DELETE FROM LobbyistDirectEmployment
WHERE year(rpt_date) > 2020
      or year(rpt_date) < 1950;

SELECT 'Adding *_ts date columns indexes to tables...' AS '';

ALTER TABLE Action ADD INDEX date_ts (`date_ts`);
ALTER TABLE Behests ADD INDEX datePaid_ts (`datePaid_ts`);
ALTER TABLE Behests ADD INDEX noticeReceived_ts (`noticeReceived_ts`);
ALTER TABLE BillVersion ADD INDEX date_ts (`date_ts`);
ALTER TABLE BillVoteSummary ADD INDEX VoteDate_ts (`VoteDate_ts`);
ALTER TABLE Contribution ADD INDEX date_ts (`date_ts`);
ALTER TABLE Gift ADD INDEX giftDate_ts (`giftDate_ts`);
ALTER TABLE Hearing ADD INDEX date_ts (`date_ts`);
ALTER TABLE HearingAgenda ADD INDEX date_created_ts (`date_created_ts`);
ALTER TABLE LegOfficePersonnel ADD INDEX start_date_ts (`start_date_ts`);
ALTER TABLE LegOfficePersonnel ADD INDEX end_date_ts (`end_date_ts`);
ALTER TABLE LegStaffGifts ADD INDEX date_given_ts (`date_given_ts`);
ALTER TABLE LobbyingContracts ADD INDEX rpt_date_ts (`rpt_date_ts`);
ALTER TABLE LobbyingFirmState ADD INDEX rpt_date_ts (`rpt_date_ts`);
ALTER TABLE LobbyistDirectEmployment ADD INDEX rpt_date_ts (`rpt_date_ts`);
ALTER TABLE LobbyistEmployment ADD INDEX rpt_date_ts (`rpt_date_ts`);
ALTER TABLE LobbyistRepresentation ADD INDEX hearing_date_ts (`hearing_date_ts`);
ALTER TABLE Term ADD INDEX start_ts (`start_ts`);
ALTER TABLE Term ADD INDEX end_ts (`end_ts`);

# SELECT 'Setting *_ts date column values...' AS '';
#
# UPDATE Action SET date_ts=TO_SECONDS(date) - TO_SECONDS('1970-01-01');
# UPDATE Behests SET datePaid_ts=TO_SECONDS(datePaid) - TO_SECONDS('1970-01-01');
# UPDATE Behests SET noticeReceived_ts=TO_SECONDS(noticeReceived) - TO_SECONDS('1970-01-01');
# UPDATE BillVersion SET date_ts=TO_SECONDS(date) - TO_SECONDS('1970-01-01');
# UPDATE BillVoteSummary SET VoteDate_ts=TO_SECONDS(VoteDate) - TO_SECONDS('1970-01-01');
# UPDATE Contribution SET date_ts=TO_SECONDS(date) - TO_SECONDS('1970-01-01');
# UPDATE Gift SET giftDate_ts=TO_SECONDS(giftDate) - TO_SECONDS('1970-01-01');
# UPDATE Hearing SET date_ts=TO_SECONDS(date) - TO_SECONDS('1970-01-01');
# UPDATE HearingAgenda SET date_created_ts=TO_SECONDS(date_created) - TO_SECONDS('1970-01-01');
# UPDATE LegOfficePersonnel SET start_date_ts=TO_SECONDS(start_date) - TO_SECONDS('1970-01-01');
# UPDATE LegOfficePersonnel SET end_date_ts=TO_SECONDS(end_date) - TO_SECONDS('1970-01-01');
# UPDATE LegStaffGifts SET date_given_ts=TO_SECONDS(date_given) - TO_SECONDS('1970-01-01');
# UPDATE LobbyingContracts SET rpt_date_ts=TO_SECONDS(rpt_date) - TO_SECONDS('1970-01-01');
# UPDATE LobbyingFirmState SET rpt_date_ts=TO_SECONDS(rpt_date) - TO_SECONDS('1970-01-01');
# UPDATE LobbyistDirectEmployment SET rpt_date_ts=TO_SECONDS(rpt_date) - TO_SECONDS('1970-01-01');
# UPDATE LobbyistEmployment SET rpt_date_ts=TO_SECONDS(rpt_date) - TO_SECONDS('1970-01-01');
# UPDATE LobbyistRepresentation SET hearing_date_ts=TO_SECONDS(hearing_date) - TO_SECONDS('1970-01-01');
# UPDATE Term SET start_ts=TO_SECONDS(start) - TO_SECONDS('1970-01-01');
# UPDATE Term SET end_ts=TO_SECONDS(end) - TO_SECONDS('1970-01-01');


-- current_term flag for Term table, current only contains a single term per legislator.
SELECT 'Altering Term table...' AS '';

ALTER TABLE Term ADD COLUMN current_term tinyint(4) NOT NULL DEFAULT '0';
ALTER TABLE Term ADD INDEX `current_term` (`current_term`);
UPDATE Term
SET current_term = 1
WHERE year = 2015;

-- BillVersionCurrent Table
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


-- GiftCombined Table
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

-- Missing indexes
SELECT 'Creating Missing Indexes...' AS '';
ALTER TABLE authors ADD INDEX vid (`vid`);
ALTER TABLE Person ADD INDEX last (`last`);
ALTER TABLE Person ADD INDEX first (`first`);
ALTER TABLE Utterance ADD INDEX current_finalized (`current`, `finalized`);

ALTER TABLE Utterance ADD INDEX time (`time`);
ALTER TABLE Utterance ADD INDEX current (`current`);
ALTER TABLE Utterance ADD INDEX finalized (`finalized`);


-- currentUtterance View
-- Andrew Note*: I have no idea why he is doing this
SELECT 'Re-Creating currentUtterance view for permissions...' AS '';
DROP VIEW currentUtterance;
CREATE VIEW `currentUtterance` AS select `Utterance`.`uid` AS `uid`,`Utterance`.`vid` AS `vid`,`Utterance`.`pid` AS `pid`,`Utterance`.`time` AS `time`,`Utterance`.`endTime` AS `endTime`,`Utterance`.`text` AS `text`,`Utterance`.`type` AS `type`,`Utterance`.`alignment` AS `alignment`,`Utterance`.`state` AS `state`,`Utterance`.`did` AS `did`,`Utterance`.`lastTouched` AS `lastTouched` from `Utterance` where ((`Utterance`.`current` = 1) and (`Utterance`.`finalized` = 1));


