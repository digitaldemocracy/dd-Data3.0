DROP EVENT IF EXISTS KnownClients_event;

DELIMITER |

CREATE EVENT KnownClients_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN

    CREATE TABLE IF NOT EXISTS NumRange
      AS
      SELECT SEQ.SeqValue
      FROM
      (
      SELECT (THOUSANDS.SeqValue +
      HUNDREDS.SeqValue +
      TENS.SeqValue +
      ONES.SeqValue) SeqValue
      FROM
      (
      SELECT 0 SeqValue
      UNION ALL
      SELECT 1 SeqValue
      UNION ALL
      SELECT 2 SeqValue
      UNION ALL
      SELECT 3 SeqValue
      UNION ALL
      SELECT 4 SeqValue
      UNION ALL
      SELECT 5 SeqValue
      UNION ALL
      SELECT 6 SeqValue
      UNION ALL
      SELECT 7 SeqValue
      UNION ALL
      SELECT 8 SeqValue
      UNION ALL
      SELECT 9 SeqValue
      ) ONES
      CROSS JOIN
      (
      SELECT 0 SeqValue
      UNION ALL
      SELECT 10 SeqValue
      UNION ALL
      SELECT 20 SeqValue
      UNION ALL
      SELECT 30 SeqValue
      UNION ALL
      SELECT 40 SeqValue
      UNION ALL
      SELECT 50 SeqValue
      UNION ALL
      SELECT 60 SeqValue
      UNION ALL
      SELECT 70 SeqValue
      UNION ALL
      SELECT 80 SeqValue
      UNION ALL
      SELECT 90 SeqValue
      ) TENS
      CROSS JOIN
      (
      SELECT 0 SeqValue
      UNION ALL
      SELECT 100 SeqValue
      UNION ALL
      SELECT 200 SeqValue
      UNION ALL
      SELECT 300 SeqValue
      UNION ALL
      SELECT 400 SeqValue
      UNION ALL
      SELECT 500 SeqValue
      UNION ALL
      SELECT 600 SeqValue
      UNION ALL
      SELECT 700 SeqValue
      UNION ALL
      SELECT 800 SeqValue
      UNION ALL
      SELECT 900 SeqValue
      ) HUNDREDS
      CROSS JOIN
      (
      SELECT 0 SeqValue
      UNION ALL
      SELECT 1000 SeqValue
      UNION ALL
      SELECT 2000 SeqValue
      UNION ALL
      SELECT 3000 SeqValue
      UNION ALL
      SELECT 4000 SeqValue
      UNION ALL
      SELECT 5000 SeqValue
      UNION ALL
      SELECT 6000 SeqValue
      UNION ALL
      SELECT 7000 SeqValue
      UNION ALL
      SELECT 8000 SeqValue
      UNION ALL
      SELECT 9000 SeqValue
      ) THOUSANDS
      ) SEQ;

    ALTER TABLE NumRange
      ADD INDEX seq_idx (SeqValue);

    DROP TABLE IF EXISTS KnownClientsTmp;
    CREATE TABLE KnownClientsTmp
    AS
      SELECT DISTINCT
        le.pid,
        o.name      AS assoc_name,
        o.oid,
        nr.SeqValue AS year,
        IF(nr.SeqValue % 2 = 1, nr.SeqValue, nr.SeqValue - 1) as session_year,
        le.state
      FROM LobbyistEmployment le
        JOIN LobbyingContracts lc
          ON lc.filer_id = le.sender_id
             AND lc.state = le.state
        JOIN NumRange nr
          ON lc.ls_beg_yr <= nr.SeqValue
             AND lc.ls_end_yr >= nr.SeqValue
        JOIN Organizations o
          ON lc.lobbyist_employer = o.oid
    WHERE lc.ls_beg_yr > 1950 and lc.ls_end_yr < 2025;

    alter table KnownClientsTmp
      add UNIQUE (pid, assoc_name, oid, year, state);

    CREATE TABLE IF NOT EXISTS KnownClients (
      pid          INT,
      assoc_name   VARCHAR(255),
      oid          INT,
      year         YEAR,
      session_year YEAR,
      state        VARCHAR(2),
      lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
      lastTouched_ts INT(11) AS ((to_seconds(`lastTouched`) - to_seconds('1970-01-01'))),

      PRIMARY KEY (pid, assoc_name, oid, year, state),

      FOREIGN KEY (pid) REFERENCES Person (pid),
      FOREIGN KEY (oid) REFERENCES Organizations (oid),
      FOREIGN KEY (state) REFERENCES State (abbrev),

      INDEX pid_idx (pid),
      INDEX oid_idx (oid)
    )
      ENGINE = INNODB
      CHARACTER SET utf8 COLLATE utf8_general_ci;

    INSERT INTO KnownClients
    (pid, assoc_name, oid, year, session_year, state)
      SELECT
        tmp.pid,
        tmp.assoc_name,
        tmp.oid,
        tmp.year,
        tmp.session_year,
        tmp.state
      FROM KnownClientsTmp tmp
        LEFT JOIN KnownClients kc
          ON kc.pid = tmp.pid
             AND kc.assoc_name = tmp.assoc_name
             AND kc.oid = tmp.oid
             AND kc.year = tmp.year
             AND kc.state = tmp.state
      WHERE kc.pid IS NULL;

    DELETE kc
    FROM KnownClients kc
      LEFT JOIN KnownClientsTmp tmp
        ON kc.pid = tmp.pid
           AND kc.assoc_name = tmp.assoc_name
           AND kc.oid = tmp.oid
           AND kc.year = tmp.year
           AND kc.state = tmp.state
    WHERE tmp.pid IS NULL;

    DROP TABLE IF EXISTS KnownClientsTmp;
    DROP TABLE IF EXISTS NumRange;

  END |

DELIMITER ;
