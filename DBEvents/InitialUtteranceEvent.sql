/*
 Creates the InitialUtterance table. Used by site to determine where to jump to for each speaker when
 looking for clips
*/
DROP EVENT IF EXISTS InitialUtterance_event;
delimiter |

CREATE EVENT InitialUtterance_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2016-09-21 07:00:00'
DO
  BEGIN

    CREATE TABLE IF NOT EXISTS InitialUtterance (
      pid INT,
      uid INT,
      did INT,

      PRIMARY KEY (pid, uid, did),
      FOREIGN KEY (pid) REFERENCES Person(pid),
      FOREIGN KEY (uid) REFERENCES Utterance(uid),
      FOREIGN KEY (did) REFERENCES BillDiscussion(did)
    );

    drop table if exists InitialUtteranceTmp;
    create TEMPORARY TABLE InitialUtteranceTmp like InitialUtterance;

    insert into InitialUtteranceTmp
    (pid, did, uid)
      select pid, did, min(uid) as uid
      from currentUtterance
      where pid is not null and did is not null
      and did > -2
      group by pid, did;

    delete t from InitialUtterance t
      left join InitialUtteranceTmp v
      on t.pid = v.pid
        and t.uid = v.uid
        and t.did = v.did
      where v.uid is null;

    insert into InitialUtterance
    (pid, did, uid)
    select pid, did, uid
    from InitialUtteranceTmp
    where (pid, did, uid) not in (select pid, did, uid
                                  from InitialUtterance);


    drop table if exists InitialUtteranceTmp;

  END |
delimiter ;


