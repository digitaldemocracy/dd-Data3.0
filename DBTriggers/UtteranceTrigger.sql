-- BillDiscussion
DROP TRIGGER IF EXISTS UtteranceSolr_BillDiscussion_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER UtteranceSolr_BillDiscussion_UpdateTrigger AFTER UPDATE ON BillDiscussion
FOR EACH ROW
  BEGIN
    UPDATE Utterance u
    SET u.dr_changed = now()
    WHERE NEW.did = u.did;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: UtteranceSolr_BillDiscussion_UpdateTrigger.
      Update to BillDiscussion w/ bid: ', NEW.bid, ' did: ', NEW.did));
  END$$

-- Hearing
DROP TRIGGER IF EXISTS UtteranceSolr_Hearing_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER UtteranceSolr_Hearing_UpdateTrigger AFTER UPDATE ON Hearing
FOR EACH ROW
  BEGIN
    UPDATE Utterance u
      join Video v
      on u.vid = v.vid
    SET u.dr_changed = now()
    WHERE NEW.hid = v.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: UtteranceSolr_Hearing_UpdateTrigger.
      Update to Hearing w/ hid: ', NEW.hid));
  END$$

-- Person
DROP TRIGGER IF EXISTS UtteranceSolr_Person_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER UtteranceSolr_Person_UpdateTrigger AFTER UPDATE ON Person
FOR EACH ROW
  BEGIN
    UPDATE Utterance u
    SET u.dr_changed = now()
    WHERE NEW.pid = u.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: UtteranceSolr_Person_UpdateTrigger.
      Update to Person w/ pid: ', NEW.pid));
  END$$
