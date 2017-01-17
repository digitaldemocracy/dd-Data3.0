/*
  Site uses this to know which BillVersion is the latest and keep track of what to display.
  Note that the lack of a dr_id makes this a really simple event
*/

DROP EVENT IF EXISTS BillVersionCurrent_event;

DELIMITER |

CREATE EVENT BillVersionCurrent_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN

    DROP TABLE IF EXISTS BillVersionCurrent;

    CREATE TABLE BillVersionCurrent LIKE BillVersion;

    INSERT INTO BillVersionCurrent
    (vid, bid, date, billState, subject, appropriation, substantive_changes, title, digest, text, state)
      SELECT vid, bid, date, billState, subject, appropriation, substantive_changes, title, digest, text, state
      FROM
        (SELECT *
         FROM BillVersion bv
         ORDER BY bv.date DESC, bv.vid ASC) bvc
      GROUP BY bvc.bid;
  END |

DELIMITER ;