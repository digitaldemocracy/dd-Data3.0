/*
  Calculates Q7 of the Analytical Queries document.
  Question:
  AUTHORSHIP STATS: For any given legislator, how many bills that they authored that were successfully passed through
  the legislature were vetoed by the Governor?

  * I also chose to show number of bills that were chaptered
*/

DROP EVENT IF EXISTS SuccessRate_event;

DELIMITER |

CREATE EVENT SuccessRate_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN
    CREATE OR REPLACE VIEW chaptered
    AS
      SELECT
        a.pid,
        b.sessionYear AS session_year,
        count(*)      AS num_chaptered
      FROM authors a
        JOIN Bill b
          ON a.bid = b.bid
      WHERE b.status = 'Chaptered'
      GROUP BY a.pid, b.sessionYear;

    CREATE OR REPLACE VIEW vetoed
    AS
      SELECT
        a.pid,
        b.sessionYear AS session_year,
        count(*)      AS num_vetoed
      FROM authors a
        JOIN Bill b
          ON a.bid = b.bid
      WHERE b.status = 'Vetoed'
      GROUP BY a.pid, b.sessionYear;

    CREATE OR REPLACE VIEW authored
    AS
      SELECT
        a.pid,
        b.sessionYear AS session_year,
        count(*)      AS num_authored
      FROM authors a
        JOIN Bill b
          ON a.bid = b.bid
      GROUP BY a.pid, b.sessionYear;


    CREATE OR REPLACE VIEW base
    AS
      SELECT DISTINCT
        pid,
        year AS session_year
      FROM Term t
      WHERE t.year >= 2015;

    CREATE TABLE IF NOT EXISTS LegislativeSuccessRates_analyt (
      pid           INT,
      session_year  YEAR,
      num_authored  INT,
      num_chaptered INT,
      num_vetoed    INT,

      PRIMARY KEY (pid, session_year),
      FOREIGN KEY (pid) REFERENCES Person (pid)
    )
      ENGINE = INNODB
      CHARACTER SET utf8
      COLLATE utf8_general_ci;

    TRUNCATE LegislativeSuccessRates_analyt;

    INSERT INTO LegislativeSuccessRates_analyt
    (pid, session_year, num_authored, num_chaptered, num_vetoed)
      SELECT
        b.pid,
        b.session_year,
        ifnull(a.num_authored, 0),
        ifnull(c.num_chaptered, 0),
        ifnull(v.num_vetoed, 0)
      FROM base b
        LEFT JOIN chaptered c
          ON b.pid = c.pid
             AND b.session_year = c.session_year
        LEFT JOIN vetoed v
          ON b.pid = v.pid
             AND b.session_year = v.session_year
        LEFT JOIN authored a
          ON b.pid = a.pid
             AND b.session_year = a.session_year;


    DROP VIEW IF EXISTS chaptered;
    DROP VIEW IF EXISTS vetoed;
    DROP VIEW IF EXISTS authored;
    DROP VIEW IF EXISTS base;

  END |

DELIMITER ;
