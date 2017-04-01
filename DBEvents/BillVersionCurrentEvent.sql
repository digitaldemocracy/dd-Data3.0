/*
  Site uses this to know which BillVersion is the latest and keep track of what to display.
*/

DROP EVENT IF EXISTS BillVersionCurrent_event;

DELIMITER |

CREATE EVENT BillVersionCurrent_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN

    CREATE TABLE IF NOT EXISTS BillVersionCurrent LIKE BillVersion;

    drop table if EXISTS BillVersionCurrentTmp;
    create TEMPORARY TABLE BillVersionCurrentTmp like BillVersion;

    -- This query is taken from Kristian
    INSERT INTO BillVersionCurrentTmp
    (vid, bid, date, billState, subject, appropriation, substantive_changes, title, digest, text, state)
      SELECT vid, bid, date, billState, subject, appropriation, substantive_changes, title, digest, text, state
      FROM
        (SELECT *
         FROM BillVersion bv
         ORDER BY bv.date DESC, bv.vid ASC) bvc
      GROUP BY bvc.bid;

    delete t from BillVersionCurrent t
      left join BillVersionCurrentTmp v
        on t.vid = v.vid
           and t.bid = v.bid
           and (t.date = v.date or (t.date is null and v.date is null))
           and t.billState = v.billState
           and (t.subject = v.subject or (t.subject is null and v.subject is null))
           and (t.appropriation = v.appropriation or (t.appropriation is null and v.appropriation is null))
           and (t.substantive_changes = v.substantive_changes or (t.substantive_changes is null and v.substantive_changes is null))
           and (t.title = v.title or (t.title is null and v.title is null))
           and (t.digest = v.digest or (t.digest is null and v.digest is null))
           and (t.text = v.text or (t.text is null and v.text is null))
           and t.state = v.state
    where v.vid is null;


    INSERT INTO BillVersionCurrent
    (vid, bid, date, billState, subject, appropriation, substantive_changes, title, digest, text, state)
      select vid, bid, date, billState, subject, appropriation, substantive_changes, title, digest, text, state
      from BillVersionCurrentTmp
      where (vid) not in (select vid
                          from BillVersionCurrent);

    drop table if exists BillVersionCurrentTmp;

  END |

DELIMITER ;