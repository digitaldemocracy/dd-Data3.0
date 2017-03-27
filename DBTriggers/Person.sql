-- PersonClassifications
DROP TRIGGER IF EXISTS PersonSol_PersonClassifications_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonClassifications_UpdateTrigger AFTER UPDATE ON PersonClassifications
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonClassifications_UpdateTrigger.
      Update to PersonClassifications w/ pid: ', NEW.pid));
  END$$

DROP TRIGGER IF EXISTS PersonSol_PersonClassifications_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonClassifications_DeleteTrigger BEFORE DELETE ON PersonClassifications
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE old.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonClassifications_DeleteTrigger.
      Delete to PersonClassifications w/ pid: ', old.pid));
  END$$

DROP TRIGGER IF EXISTS PersonSol_PersonClassifications_InsertTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonClassifications_InsertTrigger AFTER INSERT ON PersonClassifications
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonClassifications_UpdateTrigger.
      Insert to PersonClassifications w/ pid: ', NEW.pid));
  END$$

-- PersonStateAffiliation
DROP TRIGGER IF EXISTS PersonSol_PersonStateAffiliation_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonStateAffiliation_UpdateTrigger AFTER UPDATE ON PersonStateAffiliation
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonStateAffiliation_UpdateTrigger.
      Update to PersonStateAffiliation w/ pid: ', NEW.pid));
  END$$

DROP TRIGGER IF EXISTS PersonSol_PersonStateAffiliation_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonStateAffiliation_DeleteTrigger BEFORE DELETE ON PersonStateAffiliation
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE old.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonStateAffiliation_DeleteTrigger.
      Delete to PersonStateAffiliation w/ pid: ', old.pid));
  END$$

DROP TRIGGER IF EXISTS PersonSol_PersonStateAffiliation_InsertTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonStateAffiliation_InsertTrigger AFTER INSERT ON PersonStateAffiliation
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonStateAffiliation_UpdateTrigger.
      Insert to PersonStateAffiliation w/ pid: ', NEW.pid));
  END$$

-- PersonAffiliations
DROP TRIGGER IF EXISTS PersonSol_PersonAffiliations_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonAffiliations_UpdateTrigger AFTER UPDATE ON PersonAffiliations
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonAffiliations_UpdateTrigger.
      Update to PersonAffiliations w/ pid: ', NEW.pid));
  END$$

DROP TRIGGER IF EXISTS PersonSol_PersonAffiliations_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonAffiliations_DeleteTrigger BEFORE DELETE ON PersonAffiliations
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE old.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonAffiliations_DeleteTrigger.
      Delete to PersonAffiliations w/ pid: ', old.pid));
  END$$

DROP TRIGGER IF EXISTS PersonSol_PersonAffiliations_InsertTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_PersonAffiliations_InsertTrigger AFTER INSERT ON PersonAffiliations
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_PersonAffiliations_UpdateTrigger.
      Insert to PersonAffiliations w/ pid: ', NEW.pid));
  END$$

-- InitialUtterance
DROP TRIGGER IF EXISTS PersonSol_InitialUtterance_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_InitialUtterance_UpdateTrigger AFTER UPDATE ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_InitialUtterance_UpdateTrigger.
      Update to InitialUtterance w/ pid: ', NEW.pid));
  END$$

DROP TRIGGER IF EXISTS PersonSol_InitialUtterance_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_InitialUtterance_DeleteTrigger BEFORE DELETE ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE old.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_InitialUtterance_DeleteTrigger.
      Delete to InitialUtterance w/ pid: ', old.pid));
  END$$

DROP TRIGGER IF EXISTS PersonSol_InitialUtterance_InsertTrigger;
DELIMITER $$
CREATE TRIGGER PersonSol_InitialUtterance_InsertTrigger AFTER INSERT ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE Person p
    SET dr_changed = now()
    WHERE NEW.pid = p.pid;

    INSERT INTO DrChangedLogs
    (log)
    VALUES
      (CONCAT('Trigger: PersonSol_InitialUtterance_UpdateTrigger.
      Insert to InitialUtterance w/ pid: ', NEW.pid));
  END$$
