CREATE TRIGGER HearingSessionYearTrigger BEFORE INSERT ON Hearing
FOR EACH ROW
  SET NEW.session_year =
  (
    SELECT start_year from Session
      WHERE
        state = NEW.state AND start_year = SUBSTRING(NEW.date,1,4)
        OR
        state = NEW.state AND end_year = SUBSTRING(NEW.date, 1,4)
  );

DELIMITER $$

CREATE TRIGGER TempTrigger AFTER UPDATE ON B_tmp
FOR EACH ROW
  BEGIN
#     insert into tmp_log_tbl select 'hello world';
    UPDATE A_tmp 
    SET dr_changed = UNIX_TIMESTAMP(now())
    WHERE NEW.col_b = A_tmp.col_b;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('A_tmp', 'col_b', 'B_tmp', 'col_b', 'UPDATE');
  END$$

DELIMITER ;


DELIMITER $$

CREATE TRIGGER TempTrigger_insert AFTER insert ON B_tmp
FOR EACH ROW
  BEGIN
    #     insert into tmp_log_tbl select 'hello world';
    UPDATE A_tmp
    SET dr_changed = UNIX_TIMESTAMP(now())
    WHERE NEW.col_b = A_tmp.col_b;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('A_tmp', 'col_b', 'B_tmp', 'col_b', 'INSERT');
    END$$

DELIMITER ;

DELIMITER $$

CREATE TRIGGER TempTrigger_delete AFTER DELETE ON B_tmp
FOR EACH ROW
  BEGIN
    #     insert into tmp_log_tbl select 'hello world';
    UPDATE A_tmp
    SET dr_changed = UNIX_TIMESTAMP(now())
    WHERE OLD.col_b = A_tmp.col_b;

    INSERT INTO DrChangedLogs
    (`solr_tbl`, `solr_tbl_col`, `update_tbl`, `update_tbl_col`, `type`)
    VALUES
      ('A_tmp', 'col_b', 'B_tmp', 'col_b', 'DELETE');
  END$$

DELIMITER ;
