DELIMITER $$

-- Hearing
CREATE TRIGGER BillDiscussionSolr_Hearing_UpdateTrigger AFTER UPDATE ON Hearing
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion
    SET dr_changed = now()
    WHERE NEW.hid = BillDiscussion.hid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'Hearing', 'hid', 'UPDATE');
  END$$

DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Hearing_InsertTrigger AFTER INSERT ON Hearing
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion
    SET dr_changed = now()
    WHERE NEW.hid = BillDiscussion.hid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'Hearing', 'hid', 'INSERT');
  END$$

DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Hearing_DeleteTrigger AFTER DELETE ON Hearing
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion
    SET dr_changed = now()
    WHERE OLD.hid = BillDiscussion.hid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'Hearing', 'hid', 'DELETE');
  END$$


-- Committee
CREATE TRIGGER BillDiscussionSolr_Committee_UpdateTrigger AFTER UPDATE ON Committee
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion
    SET dr_changed = now()
    WHERE NEW.hid = BillDiscussion.hid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'Committee', 'hid', 'UPDATE');
  END$$

CREATE TRIGGER BillDiscussionSolr_Committee_InsertTrigger AFTER INSERT ON Committee
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion
    SET dr_changed = now()
    WHERE NEW.hid = BillDiscussion.hid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'Committee', 'hid', 'INSERT');
  END$$

CREATE TRIGGER BillDiscussionSolr_Committee_DeleteTrigger AFTER DELETE ON Committee
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion
    SET dr_changed = now()
    WHERE OLD.hid = BillDiscussion.hid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'Committee', 'hid', 'DELETE');
  END$$

DELIMITER ;


