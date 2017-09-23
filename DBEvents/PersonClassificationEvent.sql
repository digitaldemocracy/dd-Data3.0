/*
  This table is how the site keeps track of the classifications for every person.
 */

DROP EVENT IF EXISTS PersonClassifications_event;

DELIMITER |

CREATE EVENT PersonClassifications_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN
    DROP TABLE IF EXISTS NumRange;
    CREATE TABLE NumRange
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

    CREATE OR REPLACE VIEW LabeledLeg
    AS
      SELECT
        p.pid,
        p.first,
        p.middle,
        p.last,
        "Legislator" AS PersonType,
        l.state
      FROM Person p
        JOIN Legislator l
          ON p.pid = l.pid;

    -- Currently Term doesn't have values for start and end and this is fucking you
    CREATE OR REPLACE VIEW SplitTerm
    AS
      SELECT
        t1.pid,
        t1.year        AS session_year,
        YEAR(t1.start) AS specific_year
      FROM Term t1
      UNION
      SELECT
        t2.pid,
        t2.year                               AS session_year,
        IFNULL(YEAR(t2.end), year(curdate())) AS specific_year
      FROM Term t2;

    DROP TABLE IF EXISTS LabeledLegFull;
    CREATE TABLE LabeledLegFull
    AS
      SELECT
        l.*,
        st.specific_year,
        st.session_year
      FROM LabeledLeg l
        JOIN SplitTerm st
          ON l.pid = st.pid;

    DROP TABLE IF EXISTS LabeledLobbyist;
    CREATE OR REPLACE VIEW LabeledLobbyist
    AS
      SELECT DISTINCT
        p.pid,
        p.first,
        p.middle,
        p.last,
        "Lobbyist" AS PersonType,
        l.state
      FROM Person p
        JOIN Lobbyist l
          ON p.pid = l.pid;

    CREATE OR REPLACE VIEW LobTerms
    AS
      SELECT
        pid,
        nr.SeqValue                                           AS specific_year,
        IF(nr.SeqValue % 2 = 1, nr.SeqValue, nr.SeqValue - 1) AS session_year
      FROM LobbyistEmployment le
        JOIN NumRange nr
          ON le.ls_beg_yr <= nr.SeqValue
             AND le.ls_end_yr >= nr.SeqValue
      UNION
      SELECT
        pid,
        nr.SeqValue                                           AS specific_year,
        IF(nr.SeqValue % 2 = 1, nr.SeqValue, nr.SeqValue - 1) AS session_year
      FROM LobbyistDirectEmployment lde
        JOIN NumRange nr
          ON lde.ls_beg_yr <= nr.SeqValue
             AND lde.ls_end_yr >= nr.SeqValue
      UNION
      SELECT
        lr.pid,
        year(h.date)                                             AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year
      FROM LobbyistRepresentation lr
        JOIN Hearing h
          ON lr.hid = h.hid;


    DROP TABLE IF EXISTS LabeledLobFull;
    CREATE TABLE LabeledLobFull
    AS
      SELECT DISTINCT
        l.*,
        lt.specific_year,
        lt.session_year
      FROM LabeledLobbyist l
        JOIN LobTerms lt
          ON l.pid = lt.pid;

    DROP TABLE IF EXISTS LabeledGenPubFull;
    CREATE TABLE LabeledGenPubFull
    AS
      SELECT DISTINCT
        p.pid,
        p.first,
        p.middle,
        p.last,
        "General Public"                                         AS PersonType,
        year(h.date)                                             AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year,
        gp.state
      FROM Person p
        JOIN GeneralPublic gp
          ON p.pid = gp.pid
        JOIN Hearing h
          ON gp.hid = h.hid;

    DROP TABLE IF EXISTS LabeledLAOFull;
    CREATE TABLE LabeledLAOFull
    AS
      SELECT DISTINCT
        p.pid,
        p.first,
        p.middle,
        p.last,
        "Legislative Analyst Office"                             AS PersonType,
        year(h.date)                                             AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year,
        lao.state
      FROM Person p
        JOIN LegAnalystOffice lao
          ON p.pid = lao.pid
        JOIN LegAnalystOfficeRepresentation laor
          ON lao.pid = laor.pid
        JOIN Hearing h
          ON laor.hid = h.hid;


    DROP TABLE IF EXISTS LabeledStateConstFull;
    CREATE TABLE LabeledStateConstFull
    AS
      SELECT DISTINCT
        p.pid,
        p.first,
        p.middle,
        p.last,
        "State Constitutional Office"                            AS PersonType,
        YEAR(h.date)                                             AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year,
        sa.state
      FROM Person p
        JOIN StateConstOfficeRep sa
          ON p.pid = sa.pid
        JOIN StateConstOfficeRepRepresentation sar
          ON sa.pid = sar.pid
        JOIN Hearing h
          ON sar.hid = h.hid;


    DROP TABLE IF EXISTS LabeledStateAgencyFull;
    CREATE TABLE LabeledStateAgencyFull
    AS
      SELECT DISTINCT
        p.pid,
        p.first,
        p.middle,
        p.last,
        "State Agency Representative"                            AS PersonType,
        YEAR(h.date)                                             AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year,
        sa.state
      FROM Person p
        JOIN StateAgencyRep sa
          ON p.pid = sa.pid
        JOIN StateAgencyRepRepresentation sar
          ON sa.pid = sar.pid
        JOIN Hearing h
          ON sar.hid = h.hid;

    CREATE OR REPLACE VIEW LabeledLegStaff
    AS
      SELECT
        p.pid,
        p.first,
        p.middle,
        p.last,
        "Legislative Staff" AS PersonType,
        sa.state
      FROM Person p
        JOIN LegislativeStaff sa
          ON p.pid = sa.pid;

    DROP TABLE IF EXISTS SplitTermLop;
    CREATE TABLE SplitTermLop
    AS
      SELECT
        t1.staff_member     AS pid,
        year(t1.start_date) AS specific_year,
        t1.term_year        AS session_year
      FROM LegOfficePersonnel t1
      UNION
      SELECT
        t2.staff_member                            AS pid,
        IFNULL(YEAR(t2.end_date), year(curdate())) AS specific_year,
        t2.term_year                               AS session_year
      FROM LegOfficePersonnel t2;

    ALTER TABLE SplitTermLop
      ADD UNIQUE (pid, specific_year, session_year);

    DROP TABLE IF EXISTS SplitTermOp;
    CREATE TABLE SplitTermOp
    AS
      SELECT DISTINCT
        t1.staff_member                                       AS pid,
        nr.SeqValue                                           AS specific_year,
        IF(nr.SeqValue % 2 = 1, nr.SeqValue, nr.SeqValue - 1) AS session_year
      FROM OfficePersonnel t1
        JOIN NumRange nr
          ON year(t1.start_date) <= nr.SeqValue
             AND (year(t1.end_date) >= nr.SeqValue OR
                  ((t1.end_date IS NULL) AND (nr.SeqValue <= year(curdate()))));

    ALTER TABLE SplitTermOp
      ADD UNIQUE (pid, specific_year, session_year);

    CREATE OR REPLACE VIEW SplitTermCS
    AS
      SELECT
        t1.pid,
        t1.session_year     AS session_year,
        YEAR(t1.start_date) AS specific_year
      FROM ConsultantServesOn t1
      UNION
      SELECT
        t2.pid,
        t2.session_year                            AS session_year,
        IFNULL(YEAR(t2.end_date), year(curdate())) AS specific_year
      FROM ConsultantServesOn t2;

    DROP TABLE IF EXISTS LegStaffTerms;
    CREATE TABLE LegStaffTerms
    AS
      SELECT
        pid,
        specific_year,
        session_year
      FROM SplitTermLop
      UNION
      SELECT
        pid,
        specific_year,
        session_year
      FROM SplitTermOp
      UNION
      SELECT
        pid,
        specific_year,
        session_year
      FROM SplitTermCS
      UNION
      SELECT
        lr.pid,
        year(h.date)                                             AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year
      FROM LegislativeStaffRepresentation lr
        JOIN Hearing h
          ON lr.hid = h.hid;

    ALTER TABLE LegStaffTerms
      ADD INDEX pid_idx (pid);

    DROP TABLE IF EXISTS LabeledLegStaffFull;
    CREATE TABLE LabeledLegStaffFull
    AS
      SELECT DISTINCT
        ls.*,
        t.specific_year,
        t.session_year
      FROM LabeledLegStaff ls
        JOIN LegStaffTerms t
          ON ls.pid = t.pid;

    DROP TABLE IF EXISTS AllPeeps;
    CREATE TABLE AllPeeps
    AS
      SELECT
        pid,
        first,
        middle,
        last,
        PersonType,
        specific_year,
        session_year,
        state
      FROM LabeledGenPubFull
      UNION ALL
      SELECT
        pid,
        first,
        middle,
        last,
        PersonType,
        specific_year,
        session_year,
        state
      FROM LabeledLAOFull
      UNION ALL
      SELECT
        pid,
        first,
        middle,
        last,
        PersonType,
        specific_year,
        session_year,
        state
      FROM LabeledLegStaffFull
      UNION ALL
      SELECT
        pid,
        first,
        middle,
        last,
        PersonType,
        specific_year,
        session_year,
        state
      FROM LabeledStateConstFull
      UNION ALL
      SELECT
        pid,
        first,
        middle,
        last,
        PersonType,
        specific_year,
        session_year,
        state
      FROM LabeledStateAgencyFull
      UNION ALL
      SELECT
        pid,
        first,
        middle,
        last,
        PersonType,
        specific_year,
        session_year,
        state
      FROM LabeledLobFull
      UNION ALL
      SELECT
        pid,
        first,
        middle,
        last,
        PersonType,
        specific_year,
        session_year,
        state
      FROM LabeledLegFull;

    ALTER TABLE AllPeeps
      ADD INDEX pid_idx (pid);

    DROP TABLE IF EXISTS UnlabeledPeople;
    CREATE TABLE UnlabeledPeople
    AS
      SELECT DISTINCT
        u.pid,
        p.first,
        p.middle,
        p.last,
        'Unlabeled'                                              AS PersonType,
        YEAR(h.date)                                             AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year,
        v.state
      FROM currentUtterance u
        JOIN Person p
          ON u.pid = p.pid
        JOIN Video v
          ON u.vid = v.vid
        JOIN Hearing h
          ON v.hid = h.hid
        LEFT JOIN AllPeeps ap
          ON ap.pid = p.pid
      WHERE ap.pid IS NULL;

    DROP TABLE IF EXISTS PersonClassificationsTmp;
    CREATE TABLE PersonClassificationsTmp
    AS
      SELECT
        *,
        FALSE AS is_current
      FROM AllPeeps
      WHERE specific_year > 1980
      UNION ALL
      SELECT
        *,
        FALSE AS is_current
      FROM UnlabeledPeople
      WHERE specific_year > 1980;

    ALTER TABLE PersonClassificationsTmp
      ADD INDEX pk_idx (pid, PersonType, specific_year, state);

    CREATE TABLE IF NOT EXISTS PersonClassifications (
      pid            INTEGER,
      first          VARCHAR(255),
      middle         VARCHAR(255),
      last           VARCHAR(255),
      PersonType     VARCHAR(255),
      specific_year  YEAR,
      session_year   YEAR,
      state          VARCHAR(2),
      is_current     BOOL           DEFAULT FALSE,
      lastTouched    TIMESTAMP      DEFAULT NOW() ON UPDATE NOW(),
      lastTouched_ts INT(11) AS ((to_seconds(`lastTouched`) - to_seconds('1970-01-01'))),
      dr_id          INTEGER UNIQUE AUTO_INCREMENT,

      PRIMARY KEY (pid, PersonType, specific_year, state),
      FOREIGN KEY (pid) REFERENCES Person (pid),
      FOREIGN KEY (state) REFERENCES State (abbrev),

      INDEX session_year_idx (session_year),
      INDEX specific_year_idx (specific_year),
      INDEX pid_idx (pid),
      INDEX person_type_idx (PersonType),
      INDEX state_idx (state),
      INDEX is_current_idx (is_current)
    )
      ENGINE = INNODB
      CHARACTER SET utf8
      COLLATE utf8_general_ci;

    INSERT INTO PersonClassifications
    (pid, first, middle, last, PersonType, specific_year, session_year, state)
      SELECT
        tmp.pid,
        tmp.first,
        tmp.middle,
        tmp.last,
        tmp.PersonType,
        tmp.specific_year,
        tmp.session_year,
        tmp.state
      FROM PersonClassificationsTmp tmp
        LEFT JOIN PersonClassifications og
          ON tmp.pid = og.pid
             AND tmp.PersonType = og.PersonType
             AND tmp.specific_year = og.specific_year
             AND tmp.session_year = og.session_year
      WHERE og.pid IS NULL AND tmp.state IS NOT NULL;

    DELETE pc
    FROM PersonClassifications pc
      LEFT JOIN PersonClassificationsTmp tmp
        ON pc.pid = tmp.pid AND pc.PersonType = tmp.PersonType
           AND pc.specific_year = tmp.specific_year AND tmp.session_year = pc.session_year
    WHERE tmp.pid IS NULL;

    DELETE pc1 FROM PersonClassifications pc1
      JOIN PersonClassifications pc2
        ON pc1.pid = pc2.pid
           AND pc1.session_year = pc2.session_year
           AND pc1.PersonType != pc2.PersonType
    WHERE pc2.PersonType = 'Legislator';


    DROP TABLE IF EXISTS LatestClass;
    CREATE TEMPORARY TABLE LatestClass
    AS
      SELECT
        pid,
        max(specific_year) AS recent_year
      FROM PersonClassifications
      GROUP BY pid;

    ALTER TABLE LatestClass
      ADD INDEX pid_idx (pid);

    DROP TABLE IF EXISTS CurrentClassLegs;
    CREATE TEMPORARY TABLE CurrentClassLegs
    AS
      SELECT
        c.pid,
        recent_year
      FROM LatestClass c
        JOIN Term t
          ON c.pid = t.pid
      WHERE t.current_term = 1;

    ALTER TABLE CurrentClassLegs
      ADD INDEX pid_idx (pid);

    UPDATE PersonClassifications p
      JOIN CurrentClassLegs c
        ON p.pid = c.pid
           AND p.specific_year = c.recent_year
    SET is_current = TRUE
    WHERE p.PersonType = 'Legislator';

    UPDATE PersonClassifications p
      LEFT JOIN CurrentClassLegs c
        ON p.pid = c.pid
           AND p.specific_year = c.recent_year
    SET is_current = FALSE
    WHERE p.PersonType = 'Legislator' AND c.pid IS NULL;

    DROP TABLE IF EXISTS CurrentClassUtter;
    CREATE TEMPORARY TABLE CurrentClassUtter
    AS
      SELECT DISTINCT
        pid,
        session_year
      FROM currentUtterance u
        JOIN Video v
          ON u.vid = v.vid
        JOIN Hearing h
          ON v.hid = h.hid
      WHERE session_year = 2017;

    UPDATE PersonClassifications p
      JOIN CurrentClassUtter c
        ON p.pid = c.pid
           AND p.session_year = c.session_year
    SET is_current = TRUE
    WHERE p.PersonType != 'Legislator';

    UPDATE PersonClassifications p
      LEFT JOIN CurrentClassUtter c
        ON p.pid = c.pid
           AND p.session_year = c.session_year
    SET is_current = FALSE
    WHERE p.PersonType != 'Legislator' AND c.pid IS NULL;

    CREATE OR REPLACE VIEW FormerLegs
    AS
      SELECT
        pid,
        state,
        sum(is_current)
      FROM PersonClassifications
      WHERE PersonType = 'Legislator'
      GROUP BY pid, state
      HAVING sum(is_current) = 0;

    INSERT INTO PersonClassifications
    (pid, PersonType, specific_year, session_year, is_current, state)
      SELECT
        pid,
        'Former Legislator'                                               AS PersonType,
        year(curdate())                                                   AS specific_year,
        if(year(curdate()) % 2 = 0, year(curdate()) - 1, year(curdate())) AS session_year,
        1                                                                 AS is_current,
        state
      FROM FormerLegs;

    DROP TABLE IF EXISTS NumRange;
    DROP TABLE IF EXISTS CurrentClassUtter;
    DROP TABLE IF EXISTS CurrentClassLegs;

    DROP VIEW IF EXISTS LabeledLeg;
    DROP VIEW IF EXISTS SplitTerm;
    DROP VIEW IF EXISTS LabeledLegFull;

    DROP VIEW IF EXISTS LabeledLobbyist;
    DROP VIEW IF EXISTS LobTerms;
    DROP TABLE IF EXISTS LabeledLobFull;

    DROP VIEW IF EXISTS FormerLegs;

    DROP TABLE IF EXISTS LabeledGenPubFull;
    DROP TABLE IF EXISTS LabeledLAOFull;
    DROP TABLE IF EXISTS LabeledStateConstFull;
    DROP TABLE IF EXISTS LabeledStateAgencyFull;

    DROP VIEW IF EXISTS LabeledLegStaff;
    DROP VIEW IF EXISTS SplitTermLop;
    DROP VIEW IF EXISTS SplitTermOp;
    DROP VIEW IF EXISTS SplitTermCS;
    DROP TABLE IF EXISTS LegStaffTerms;
    DROP TABLE IF EXISTS LabeledLegStaffFull;

    DROP TABLE IF EXISTS AllPeeps;
    DROP TABLE IF EXISTS UnlabeledPeople;

    DROP TABLE IF EXISTS PersonClassificationsTmp;

  END |

DELIMITER ;
