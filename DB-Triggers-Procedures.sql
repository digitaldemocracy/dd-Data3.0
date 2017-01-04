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

CREATE TRIGGER TempTrigger BEFORE INSERT ON Temp
FOR EACH ROW
  SET NEW.val = (select name
                 from State
                 where abbrev = 'CA');
