-- Hearing
DROP TRIGGER IF EXISTS BillDiscussionSolr_Hearing_UpdateTrigger;
DELIMITER $$
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

# DROP TRIGGER IF EXISTS BillDiscussionSolr_Hearing_InsertTrigger;
# DELIMITER $$
# CREATE TRIGGER BillDiscussionSolr_Hearing_InsertTrigger AFTER INSERT ON Hearing
# FOR EACH ROW
#   BEGIN
#     UPDATE BillDiscussion
#     SET dr_changed = now()
#     WHERE NEW.hid = BillDiscussion.hid;
#
#     INSERT INTO DrChangedLogs
#     (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
#     VALUES
#       ('BillDiscussion', 'hid', 'Hearing', 'hid', 'INSERT');
#   END$$

# DROP TRIGGER IF EXISTS BillDiscussionSolr_Hearing_DeleteTrigger;
# DELIMITER $$
# CREATE TRIGGER BillDiscussionSolr_Hearing_DeleteTrigger AFTER DELETE ON Hearing
# FOR EACH ROW
#   BEGIN
#     UPDATE BillDiscussion
#     SET dr_changed = now()
#     WHERE OLD.hid = BillDiscussion.hid;
#
#     INSERT INTO DrChangedLogs
#     (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
#     VALUES
#       ('BillDiscussion', 'hid', 'Hearing', 'hid', 'DELETE');
#   END$$


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
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'Committee', 'hid', 'UPDATE');
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
    WHERE NEW.cid = ch.cid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'CommitteeHearings', 'hid', 'UPDATE');
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
    WHERE NEW.cid = ch.cid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'CommitteeHearings', 'hid', 'INSERT');
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
    WHERE OLD.cid = ch.cid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'hid', 'CommitteeHearings', 'hid', 'DELETE');
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
    SET bd.dr_changed = now()
    WHERE NEW.vid = bd.startVideo;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'startVideo', 'Video', 'vid', 'UPDATE');
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
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'bid', 'Bill', 'bid', 'UPDATE');
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
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'bid', 'authors', 'bid', 'INSERT');
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
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'bid', 'authors', 'bid', 'UPDATE');
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
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'bid', 'Person', 'pid', 'UPDATE');
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
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'bid', 'authors', 'bid', 'DELETE');
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
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'did', 'InitialUtterance', 'did', 'INSERT');
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_InitialUtterance_UpdateTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_InitialUtterance_UpdateTrigger
AFTER UPDATE ON InitialUtterance
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE NEW.bid = bd.bid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'bid', 'authors', 'bid', 'UPDATE');
  END$$

DROP TRIGGER IF EXISTS BillDiscussionSolr_InitialUtterance_DeleteTrigger;
DELIMITER $$
CREATE TRIGGER BillDiscussionSolr_Authors_DeleteTrigger
BEFORE DELETE ON authors
FOR EACH ROW
  BEGIN
    UPDATE BillDiscussion bd
    SET bd.dr_changed = now()
    WHERE OLD.bid = bd.bid;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('BillDiscussion', 'bid', 'authors', 'bid', 'DELETE');
  END$$
