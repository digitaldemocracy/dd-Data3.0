/*
  Calculates answers to questions 12-18 of the Analytical Queries document.
 */
DROP EVENT IF EXISTS Coauthorship_event;

DELIMITER |

CREATE EVENT Coauthorship_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN

    CREATE OR REPLACE VIEW BillCoauthors
    AS
      SELECT
        a.pid,
        a.bid,
        a.vid,
        bs.pid        AS co_pid,
        t1.party      AS party,
        t2.party      AS co_party,
        b.state,
        b.sessionYear AS session_year
      FROM authors a
        JOIN BillSponsors bs
          ON a.bid = bs.bid
             AND a.vid = bs.vid
        JOIN Bill b
          ON a.bid = b.bid
        JOIN Term t1
          ON a.pid = t1.pid
             AND t1.year = b.sessionYear
        JOIN Term t2
          ON bs.pid = t2.pid
             AND t2.year = b.sessionYear
      WHERE (bs.contribution = 'Coauthor' OR bs.contribution = 'Principal Coauthor');

    CREATE OR REPLACE VIEW TotalCoauthors
    AS
      SELECT
        pid,
        session_year,
        count(*) AS count
      FROM BillCoauthors
      GROUP BY pid, session_year;

    CREATE OR REPLACE VIEW SamePartyCoauthors
    AS
      SELECT
        pid,
        session_year,
        count(*) AS count
      FROM BillCoauthors
      WHERE party = co_party
      GROUP BY pid, session_year;

    CREATE OR REPLACE VIEW DiffPartyCoauthors
    AS
      SELECT
        pid,
        session_year,
        count(*) AS count
      FROM BillCoauthors
      WHERE party != co_party
      GROUP BY pid, session_year;


    CREATE OR REPLACE VIEW TotalCoauthorship
    AS
      SELECT
        co_pid,
        session_year,
        count(*) AS count
      FROM BillCoauthors
      GROUP BY co_pid, session_year;

    CREATE OR REPLACE VIEW SamePartyCoauthorship
    AS
      SELECT
        co_pid,
        session_year,
        count(*) AS count
      FROM BillCoauthors
      WHERE party = co_party
      GROUP BY co_pid, session_year;

    CREATE OR REPLACE VIEW DiffPartyCoauthorship
    AS
      SELECT
        co_pid,
        session_year,
        count(*) AS count
      FROM BillCoauthors
      WHERE party != co_party
      GROUP BY co_pid, session_year;


    CREATE TABLE IF NOT EXISTS Coauthors_analyt (
      pid                      INT,
      session_year             YEAR,
      total_coauthors          INT, -- number of OTHER legs who were coauthors on this leg's bills
      same_party_coauthors     INT,
      diff_party_coauthors     INT,
      total_coauthorships      INT, -- number of bills THIS leg was a coauthor on
      same_party_coauthorships INT,
      diff_party_coauthorships INT,

      PRIMARY KEY (pid, session_year),
      FOREIGN KEY (pid) REFERENCES Person (pid)
    )
      ENGINE = InnoDB
      DEFAULT CHARSET = utf8;

    TRUNCATE Coauthors_analyt;

    INSERT INTO Coauthors_analyt
      SELECT
        t.pid,
        t.session_year,
        t.count as total_coauthors,
        s.count as same_party_coauthors,
        d.count as diff_party_coauthors,
        th.count as total_coauthorships,
        sh.count as same_party_coauthorships,
        dh.count as diff_party_coauthorships
      FROM TotalCoauthors t
        JOIN SamePartyCoauthors s
          ON t.pid = s.pid
             AND t.session_year = s.session_year
        JOIN DiffPartyCoauthors d
          ON t.pid = d.pid
             AND t.session_year = d.session_year
        JOIN TotalCoauthorship th
          ON t.pid = th.co_pid
             AND t.session_year = th.session_year
        JOIN SamePartyCoauthorship sh
          ON t.pid = sh.co_pid
             AND t.session_year = sh.session_year
        JOIN DiffPartyCoauthorship dh
          ON t.pid = dh.co_pid
             AND t.session_year = dh.session_year;

    drop view if exists BillCoauthors;
    drop view if exists TotalCoauthors;
    drop view if exists SamePartyCoauthors;
    drop view if exists DiffPartyCoauthors;
    drop view if exists TotalCoauthorship;
    drop view if exists SamePartyCoauthorship;
    drop view if exists DiffPartyCoauthorship;

  END |

DELIMITER ;
