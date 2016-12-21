## The following are triggers created to update the timestamps for drupal

DELIMITER $$
DROP TRIGGER IF EXISTS Action_ts_trigger$$
CREATE TRIGGER Action_ts_trigger
BEFORE INSERT ON Action
FOR EACH ROW
  BEGIN
    SET NEW.date_ts = UNIX_TIMESTAMP(NEW.date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS Behests_ts_trigger$$
CREATE TRIGGER Behests_ts_trigger
BEFORE INSERT ON Behests
FOR EACH ROW
  BEGIN
    SET NEW.datePaid_ts = UNIX_TIMESTAMP(NEW.datePaid),
        NEW.noticeReceived_ts = UNIX_TIMESTAMP(NEW.noticeReceived);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS BillVersion_ts_trigger$$
CREATE TRIGGER BillVersion_ts_trigger
BEFORE INSERT ON BillVersion
FOR EACH ROW
  BEGIN
    SET NEW.date_ts = UNIX_TIMESTAMP(NEW.date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS BillVoteSummary_ts_trigger$$
CREATE TRIGGER BillVoteSummary_ts_trigger
BEFORE INSERT ON BillVoteSummary
FOR EACH ROW
  BEGIN
    SET NEW.VoteDate_ts = UNIX_TIMESTAMP(NEW.VoteDate);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS Contribution_ts_trigger$$
CREATE TRIGGER Contribution_ts_trigger
BEFORE INSERT ON Contribution
FOR EACH ROW
  BEGIN
    SET NEW.date_ts = UNIX_TIMESTAMP(NEW.date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS Gift_ts_trigger$$
CREATE TRIGGER Gift_ts_trigger
BEFORE INSERT ON Gift
FOR EACH ROW
  BEGIN
    SET NEW.giftDate_ts = UNIX_TIMESTAMP(NEW.giftDate);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS Hearing_ts_trigger$$
CREATE TRIGGER Hearing_ts_trigger
BEFORE INSERT ON Hearing
FOR EACH ROW
  BEGIN
    SET NEW.date_ts = UNIX_TIMESTAMP(NEW.date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS HearingAgenda_ts_trigger$$
CREATE TRIGGER HearingAgenda_ts_trigger
BEFORE INSERT ON HearingAgenda
FOR EACH ROW
  BEGIN
    SET NEW.date_created_ts = UNIX_TIMESTAMP(NEW.date_created);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS LegOfficePersonnel_ts_trigger$$
CREATE TRIGGER LegOfficePersonnel_ts_trigger
BEFORE INSERT ON LegOfficePersonnel
FOR EACH ROW
  BEGIN
    SET NEW.start_date_ts = UNIX_TIMESTAMP(NEW.start_date),
        NEW.end_date_ts = UNIX_TIMESTAMP(NEW.end_date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS LegStaffGifts_ts_trigger$$
CREATE TRIGGER LegStaffGifts_ts_trigger
BEFORE INSERT ON LegStaffGifts
FOR EACH ROW
  BEGIN
    SET NEW.date_given_ts = UNIX_TIMESTAMP(NEW.date_given);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS LobbyingContracts_ts_trigger$$
CREATE TRIGGER LobbyingContracts_ts_trigger
BEFORE INSERT ON LobbyingContracts
FOR EACH ROW
  BEGIN
    SET NEW.rpt_date_ts = UNIX_TIMESTAMP(NEW.rpt_date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS LobbyingFirmState_ts_trigger$$
CREATE TRIGGER LobbyingFirmState_ts_trigger
BEFORE INSERT ON LobbyingFirmState
FOR EACH ROW
  BEGIN
    SET NEW.rpt_date_ts = UNIX_TIMESTAMP(NEW.rpt_date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS LobbyistDirectEmployment_ts_trigger$$
CREATE TRIGGER LobbyistDirectEmployment_ts_trigger
BEFORE INSERT ON LobbyistDirectEmployment
FOR EACH ROW
  BEGIN
    SET NEW.rpt_date_ts = UNIX_TIMESTAMP(NEW.rpt_date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS LobbyistEmployment_ts_trigger$$
CREATE TRIGGER LobbyistEmployment_ts_trigger
BEFORE INSERT ON LobbyistEmployment
FOR EACH ROW
  BEGIN
    SET NEW.rpt_date_ts = UNIX_TIMESTAMP(NEW.rpt_date);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS LobbyistRepresentation_ts_trigger$$
CREATE TRIGGER LobbyistRepresentation_ts_trigger
BEFORE INSERT ON LobbyistRepresentation
FOR EACH ROW
  BEGIN
    SET NEW.hearing_date_ts = UNIX_TIMESTAMP(NEW.hearing_date_ts);
  END$$
DELIMITER ;

DELIMITER $$
DROP TRIGGER IF EXISTS Term_ts_trigger$$
CREATE TRIGGER Term_ts_trigger
BEFORE INSERT ON Term
FOR EACH ROW
  BEGIN
    SET NEW.start_ts = UNIX_TIMESTAMP(NEW.start),
        NEW.end_ts = UNIX_TIMESTAMP(NEW.end);
  END$$
DELIMITER ;
