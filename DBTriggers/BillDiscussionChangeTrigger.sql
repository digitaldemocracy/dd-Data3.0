-- Hearing
DROP TRIGGER IF EXISTS BillDiscussionSolr_Hearing_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Hearing_UpdateTrigger AFTER UPDATE ON Hearing
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET dr_changed = now()
    WHERE NEW.hid = BillDiscussion.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_Hearing_UpdateTrigger.
      Update to Hearing w/ hid: ', NEW.hid));
  END$$

DELIMITER ;

-- Committee
DROP TRIGGER IF EXISTS BillDiscussionSolr_Committee_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Committee_UpdateTrigger AFTER UPDATE ON Committee
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
      JOIN CommitteeHearings ch
        ON bd.hid = ch.hid
      join Committee c
        ON ch.cid = c.cid
    SET bd.dr_changed = now()
    WHERE NEW.cid = ch.cid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_Committee_UpdateTrigger
       Update to Committee w/ cid: ', NEW.cid));
  END$$


DROP TRIGGER IF EXISTS BillDiscussionSolr_CommitteeHearings_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_CommitteeHearings_UpdateTrigger
AFTER UPDATE ON CommitteeHearings
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
      JOIN CommitteeHearings ch
        ON bd.hid = ch.hid
    SET bd.dr_changed = now()
    WHERE NEW.cid = ch.cid
      AND NEW.hid = ch.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_CommitteeHearings_UpdateTrigger
       Update to CommitteeHearings w/ cid: ', NEW.cid, ' hid: ', NEW.hid));
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_CommitteeHearings_InsertTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_CommitteeHearings_InsertTrigger
AFTER INSERT ON CommitteeHearings
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
      JOIN CommitteeHearings ch
        ON bd.hid = ch.hid
    SET bd.dr_changed = now()
    WHERE NEW.cid = ch.cid
      AND NEW.hid = ch.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_CommitteeHearings_InsertTrigger
       Insert to CommitteeHearings w/ cid: ', NEW.cid, ' hid: ', NEW.hid));
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_CommitteeHearings_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_CommitteeHearings_DeleteTrigger
BEFORE DELETE ON CommitteeHearings
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
      JOIN CommitteeHearings ch
        ON bd.hid = ch.hid
    SET bd.dr_changed = now()
    WHERE OLD.cid = ch.cid
      AND OLD.hid = ch.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_CommitteeHearings_DeleteTrigger
       Delete from CommitteeHearings w/ cid: ', OLD.cid, ' hid: ', OLD.hid));
  END$$

DELIMITER ;

-- Video
DROP TRIGGER IF EXISTS BillDiscussionSolr_Video_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Video_UpdateTrigger AFTER UPDATE ON Video
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
      JOIN Video v
        ON bd.startVideo = v.vid
          OR bd.endVideo = v.vid
    SET bd.dr_changed = now()
    WHERE NEW.vid = bd.startVideo
      OR bd.endVideo = v.vid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_Video_UpdateTrigger
       Update on Video w/ vid: ', NEW.vid));
  END$$

-- Bill
DROP TRIGGER IF EXISTS BillDiscussionSolr_Bill_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Bill_UpdateTrigger AFTER UPDATE ON Bill
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE NEW.bid = bd.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_Bill_UpdateTrigger
       Update on Bill w/ bid: ', NEW.bid));
  END$$


-- Person and Authors
DROP TRIGGER IF EXISTS BillDiscussionSolr_Authors_InsertTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Authors_InsertTrigger
AFTER INSERT ON authors
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE NEW.bid = bd.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_Authors_InsertTrigger
       Insert on authors w/ pid: ', NEW.pid, ' bid: ', NEW.bid));
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_Authors_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Authors_UpdateTrigger
AFTER UPDATE ON authors
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE NEW.bid = bd.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_Authors_UpdateTrigger
       Update on authors w/ pid: ', NEW.pid, ' bid: ', NEW.bid));
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_Authors_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Authors_DeleteTrigger
BEFORE DELETE ON authors
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE OLD.bid = bd.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_Authors_DeleteTrigger
       Update on authors w/ pid: ', OLD.pid, ' bid: ', OLD.bid));
  END$$


DROP TRIGGER IF EXISTS BillDiscussionSolr_PersonAuthors_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_PersonAuthors_UpdateTrigger
AFTER UPDATE ON Person
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
      JOIN authors a
        ON bd.bid = a.bid
    SET bd.dr_changed = now()
    WHERE NEW.pid = a.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_PersonAuthors_UpdateTrigger
       Update on Person w/ pid: ', NEW.pid));
  END$$

-- Initial Utterance trigger
DROP TRIGGER IF EXISTS BillDiscussionSolr_InitialUtterance_InsertTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_InitialUtterance_InsertTrigger
AFTER INSERT ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE NEW.did = bd.did;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_InitialUtterance_InsertTrigger
       Insert on InitialUtterance w/ uid: ', NEW.uid));

  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_InitialUtterance_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_InitialUtterance_UpdateTrigger
AFTER UPDATE ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE NEW.did = bd.did;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_InitialUtterance_UpdateTrigger
       Update on InitialUtterance w/ uid: ', NEW.uid));
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_InitialUtterance_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_InitialUtterance_DeleteTrigger
BEFORE DELETE ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE OLD.did = bd.did;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_InitialUtterance_DeleteTrigger
       Delete from InitialUtterance w/ uid: ', OLD.uid));
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_InitialUtterancePerson_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_InitialUtterancePerson_UpdateTrigger
AFTER UPDATE ON Person
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
      JOIN InitialUtterance u
        ON bd.did = u.did
      JOIN Person p
        ON p.pid = u.pid
    SET bd.dr_changed = now()
    WHERE NEW.pid = u.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_InitialUtterancePerson_UpdateTrigger
       Update on Person w/ pid: ', NEW.pid));
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_BillVersion_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_BillVersion_UpdateTrigger
AFTER UPDATE ON BillVersion
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
      JOIN BillVersion bv
        ON bv.bid = bv.bid
    SET bd.dr_changed = now()
    WHERE NEW.vid = bv.vid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillDiscussionSolr_BillVersion_UpdateTrigger
       Update on BillVersion w/ vid: ', NEW.bid));
  END$$
