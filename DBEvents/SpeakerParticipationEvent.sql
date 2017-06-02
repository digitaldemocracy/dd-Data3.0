-- Creates the SpeakerParticipation table

DROP EVENT IF EXISTS SpeakerParticipation_event;
delimiter |

CREATE EVENT SpeakerParticipation_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2016-09-21 07:00:00'
DO
  BEGIN

    DROP TABLE IF EXISTS UtterInfo;
    DROP TABLE IF EXISTS SpeakerParticipation;

    CREATE TEMPORARY TABLE UtterInfo
    AS
      SELECT u.uid, u.pid, v.hid, u.did,
        LENGTH(u.text) - LENGTH(REPLACE(u.text, ' ', ''))+1 AS WordCount,
        u.endTime - u.time AS Time,
        u.state
      FROM currentUtterance u
        JOIN Video v
          ON u.vid = v.vid;

    ALTER TABLE UtterInfo ADD INDEX idx (pid);

    CREATE TABLE SpeakerParticipation
    AS
      SELECT p.pid,
        h.session_year,
        u.state,
        SUM(WordCount) AS WordCountTotal,
        SUM(WordCount) / COUNT(DISTINCT h.hid) AS WordCountHearingAvg,
        SUM(Time) AS TimeTotal,
        SUM(Time) / COUNT(DISTINCT h.hid) AS TimeHearingAvg,
        COUNT(DISTINCT did) AS BillDiscussionCount
      FROM UtterInfo u
        JOIN Hearing h
          ON u.hid = h.hid
        JOIN (SELECT DISTINCT pid, session_year FROM PersonClassifications WHERE PersonType != 'Legislator') p
          ON u.pid = p.pid
             AND h.session_year = p.session_year
      WHERE p.pid not in (4468)
      GROUP BY p.pid, h.session_year, u.state
      ORDER BY BillDiscussionCount DESC;

    DROP TABLE IF EXISTS UtterInfo;

    ALTER TABLE SpeakerParticipation ADD COLUMN dr_id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY;

  END |
delimiter ;
