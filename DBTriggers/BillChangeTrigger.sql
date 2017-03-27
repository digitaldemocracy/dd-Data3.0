DROP TRIGGER IF EXISTS BillSolr_authors_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillSolr_authors_UpdateTrigger AFTER UPDATE ON authors
FOR EACH ROW
  BEGIN
    UPDATE Bill b
    SET dr_changed = now()
    WHERE NEW.bid = b.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillSolr_authors_UpdateTrigger.
      Update to authors w/ bid: ', NEW.bid, ' pid: ', NEW.pid));
  END$$

DROP TRIGGER IF EXISTS BillSolr_authors_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER BillSolr_authors_DeleteTrigger BEFORE DELETE ON authors
FOR EACH ROW
  BEGIN
    UPDATE Bill b
    SET dr_changed = now()
    WHERE OLD.bid = b.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillSolr_authors_DeleteTrigger.
      Delete to authors w/ bid: ', old.bid, ' pid: ', old.pid));
  END$$

DROP TRIGGER IF EXISTS BillSolr_authors_InsertTrigger;
DELIMITER $$
CREATE TRIGGER BillSolr_authors_InsertTrigger AFTER INSERT ON authors
FOR EACH ROW
  BEGIN
    UPDATE Bill b
    SET dr_changed = now()
    WHERE NEW.bid = b.bid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: BillSolr_authors_UpdateTrigger.
      Insert to authors w/ bid: ', NEW.bid, ' pid: ', NEW.pid));
  END$$
