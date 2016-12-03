drop table InitialUtterance;

CREATE TABLE IF NOT EXISTS InitialUtterance (
  pid INT,
  uid INT,
  did INT,

  PRIMARY KEY (pid, uid, did),
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (uid) REFERENCES Utterance(uid),
  FOREIGN KEY (did) REFERENCES BillDiscussion(did)
);


insert into InitialUtterance
(pid, did, uid)
select pid, did, min(uid) as uid
from currentUtterance
group by pid, did;

alter table Utterance
  drop current_utterance_flag;

CREATE OR REPLACE VIEW currentUtterance
AS SELECT uid, vid, pid, time, endTime, text, type, alignment, state, did,
     lastTouched
   FROM Utterance
   WHERE current = TRUE AND finalized = TRUE ORDER BY time DESC;