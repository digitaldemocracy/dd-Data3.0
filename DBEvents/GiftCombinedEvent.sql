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
INSERT INTO GiftCombined (year, RecordId, recipientPid, schedule, sourceName,
                          activity, sourceCity, sourceState, giftValue,
                          giftDate, reimbursed, giftIncomeFlag, speechFlag,
                          description, sessionYear, state, lastTouched, oid,
                          giftDate_ts)
  SELECT YEAR(giftDate), RecordId, pid, schedule, sourceName,
    activity, city, cityState, value,
    giftDate, reimbursed, giftIncomeFlag, speechFlag,
    description, sessionYear, state, lastTouched, oid, giftDate_ts
  from Gift;
INSERT INTO GiftCombined (year, agencyName, recipientPid, legislatorPid,
                          position, districtNumber, jurisdiction, sourceName,
                          sourceCity, sourceState, sourceBusiness, giftDate,
                          giftValue, reimbursed, description, speechOrPanel,
                          imageUrl, lastTouched, schedule, sessionYear, giftDate_ts)
  select year, agency_name, staff_member, legislator,
    position, district_number, jurisdiction, source_name,
    source_city, source_state, source_business, date_given,
    gift_value, reimbursed, gift_description, speech_or_panel,
    image_url, lastTouched, schedule, session_year, date_given_ts
  from LegStaffGifts;
-- @TODO FIX LegStaffGifts - Temporary set state to CA for LegStaffGifts, since it doesn't contain a state yet
UPDATE GiftCombined set state='CA' where state IS NULL;

alter table GiftCombined
  add Index session_year_idx (sessionYear);

alter table GiftCombined
  add index oid_idx (oid);
