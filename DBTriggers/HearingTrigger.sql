-- CommitteeHearing
DROP TRIGGER IF EXISTS HearingSolr_CommitteeHearings_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_CommitteeHearings_UpdateTrigger AFTER UPDATE ON CommitteeHearings
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
    SET h.dr_changed = now()
    WHERE NEW.hid = h.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_CommitteeHearings_UpdateTrigger.
      Update to CommitteeHearing w/ hid: ', NEW.hid, ' cid: ', NEW.cid));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_CommitteeHearings_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_CommitteeHearings_DeleteTrigger BEFORE DELETE ON CommitteeHearings
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
    SET h.dr_changed = now()
    WHERE old.hid = h.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_CommitteeHearings_DeleteTrigger.
      Delete to CommitteeHearing w/ hid: ', old.hid, ' cid: ', old.cid));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_CommitteeHearings_InsertTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_CommitteeHearings_InsertTrigger AFTER INSERT ON CommitteeHearings
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
    SET h.dr_changed = now()
    WHERE NEW.hid = h.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_CommitteeHearings_InsertTrigger.
      Insert to CommitteeHearing w/ hid: ', NEW.hid, ' cid: ', NEW.cid));
  END$$

-- Committee
DROP TRIGGER IF EXISTS HearingSolr_Committee_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_Committee_UpdateTrigger AFTER UPDATE ON Committee
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join CommitteeHearings ch
      on h.cid = ch.hid
    SET h.dr_changed = now()
    WHERE NEW.cid = h.cid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_Committee_UpdateTrigger.
      Update to Committee w/ cid: ', NEW.cid));
  END$$

-- BillDiscussion
DROP TRIGGER IF EXISTS HearingSolr_BillDiscussion_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_BillDiscussion_UpdateTrigger AFTER UPDATE ON BillDiscussion
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
    SET h.dr_changed = now()
    WHERE NEW.hid = h.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_BillDiscussion_UpdateTrigger.
      Update to BillDiscussion w/ did: ', NEW.did));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_BillDiscussion_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_BillDiscussion_DeleteTrigger BEFORE DELETE ON BillDiscussion
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
    SET h.dr_changed = now()
    WHERE old.hid = h.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_BillDiscussion_DeleteTrigger.
      Delete to BillDiscussion w/ did: ', OLD.did));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_BillDiscussion_InsertTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_BillDiscussion_InsertTrigger AFTER INSERT ON BillDiscussion
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
    SET h.dr_changed = now()
    WHERE NEW.hid = h.hid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_BillDiscussion_InsertTrigger.
      Insert to BillDiscussion w/ did: ', NEW.did));
  END$$

-- InitialUtterance
DROP TRIGGER IF EXISTS HearingSolr_InitialUtterance_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_InitialUtterance_UpdateTrigger AFTER UPDATE ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
      on h.hid = bd.hid
      join InitialUtterance iu
      on bd.did = iu.did
    SET h.dr_changed = now()
    WHERE NEW.did = bd.did;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_InitialUtterance_UpdateTrigger.
      Update to InitialUtterance w/ uid: ', NEW.uid));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_InitialUtterance_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_InitialUtterance_DeleteTrigger BEFORE DELETE ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
        on h.hid = bd.hid
      join InitialUtterance iu
        on bd.did = iu.did
    SET h.dr_changed = now()
    WHERE old.did = bd.did;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_InitialUtterance_DeleteTrigger.
      Delete to InitialUtterance w/ uid: ', old.uid));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_InitialUtterance_InsertTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_InitialUtterance_InsertTrigger AFTER INSERT ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
        on h.hid = bd.hid
      join InitialUtterance iu
        on bd.did = iu.did
    SET h.dr_changed = now()
    WHERE NEW.did = bd.did;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_InitialUtterance_InsertTrigger.
      Insert to InitialUtterance w/ uid: ', NEW.uid));
  END$$


-- Person
DROP TRIGGER IF EXISTS HearingSolr_Person_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_Person_UpdateTrigger AFTER UPDATE ON Person
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
        on h.hid = bd.hid
      join InitialUtterance iu
        on bd.did = iu.did
    SET h.dr_changed = now()
    WHERE NEW.pid = iu.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_Person_UpdateTrigger.
      Update to Person w/ pid: ', NEW.pid));
  END$$

-- Bill
DROP TRIGGER IF EXISTS HearingSolr_Bill_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_Bill_UpdateTrigger AFTER UPDATE ON Bill
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
        on h.hid = bd.hid
    SET h.dr_changed = now()
    WHERE NEW.bid = bd.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_Bill_UpdateTrigger.
      Update to Bill w/ bid: ', NEW.bid));
  END$$

-- authors
DROP TRIGGER IF EXISTS HearingSolr_authors_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_authors_UpdateTrigger AFTER UPDATE ON authors
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
      on h.hid = bd.hid
      join authors a
      on bd.bid = a.bid
    SET h.dr_changed = now()
    WHERE NEW.bid = a.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_authors_UpdateTrigger.
      Update to authors w/ bid: ', NEW.bid, ' pid: ', new.pid));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_authors_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_authors_DeleteTrigger BEFORE DELETE ON authors
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
        on h.hid = bd.hid
      join authors a
        on bd.bid = a.bid
    SET h.dr_changed = now()
    WHERE old.bid = a.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_authors_DeleteTrigger.
      Delete to authors w/ bid: ', old.bid, ' pid: ', old.pid));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_authors_InsertTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_authors_InsertTrigger AFTER INSERT ON authors
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
        on h.hid = bd.hid
      join authors a
        on bd.bid = a.bid
    SET h.dr_changed = now()
    WHERE NEW.bid = a.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_authors_InsertTrigger.
      Insert to authors w/ bid: ', NEW.bid, ' pid: ', new.pid));
  END$$

DROP TRIGGER IF EXISTS HearingSolr_authors_person_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER HearingSolr_authors_person_UpdateTrigger AFTER UPDATE ON Person
FOR EACH ROW
  BEGIN
    UPDATE Hearing h
      join BillDiscussion bd
        on h.hid = bd.hid
      join authors a
        on bd.bid = a.bid
      join Person p
        on a.pid = p.pid
    SET h.dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: HearingSolr_authors_UpdateTrigger.
      Update to Person w/ pid: ', NEW.pid));
  END$$

