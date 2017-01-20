/*
  This table is how the site keeps track of the classifications for every person.
 */

DROP EVENT IF EXISTS PersonClassificationsEvent_event;

DELIMITER |

CREATE EVENT PersonClassificationsEvent_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN
    DROP TABLE IF EXISTS NumRange;
    CREATE TABLE NumRange
    AS
      SELECT
        SEQ.SeqValue
      FROM
        (
          SELECT
            (THOUSANDS.SeqValue +
             HUNDREDS.SeqValue +
             TENS.SeqValue +
             ONES.SeqValue) SeqValue
          FROM
            (
              SELECT 0  SeqValue
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

    alter table NumRange
        add index seq_idx (SeqValue);

    CREATE OR REPLACE VIEW LabeledLeg
    AS
      SELECT p.pid,
        p.first, p.middle, p.last,
        "Legislator" AS PersonType,
        l.state
      FROM Person p
      JOIN Legislator l
      ON p.pid = l.pid;

    -- Currently Term doesn't have values for start and end and this is fucking you
    CREATE OR REPLACE VIEW SplitTerm
      AS
    SELECT t1.pid, t1.year as session_year, YEAR(t1.start) as specific_year
    FROM Term t1
    UNION
    SELECT t2.pid, t2.year as session_year, IFNULL(YEAR(t2.end), year(curdate())) as specific_year
    FROM Term t2;

    drop table if EXISTS LabeledLegFull;
    create table LabeledLegFull
      AS
    SELECT l.*,
      st.specific_year,
      st.session_year
    FROM LabeledLeg l
      JOIN SplitTerm st
      ON l.pid = st.pid;

    drop table if exists LabeledLobbyist;
    CREATE OR REPLACE VIEW LabeledLobbyist
    AS
      SELECT DISTINCT p.pid,
        p.first, p.middle, p.last,
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


    drop table if exists LabeledLobFull;
    create table LabeledLobFull
    AS
      SELECT distinct l.*,
        lt.specific_year,
        lt.session_year
      FROM LabeledLobbyist l
        JOIN LobTerms lt
          ON l.pid = lt.pid;

    drop table if exists LabeledGenPubFull;
    CREATE table LabeledGenPubFull
    AS
      SELECT DISTINCT p.pid,
        p.first, p.middle, p.last,
        "General Public" AS PersonType,
        year(h.date) AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year,
        gp.state
      FROM Person p
        JOIN GeneralPublic gp
          ON p.pid = gp.pid
        JOIN Hearing h
        ON gp.hid = h.hid;

    drop table if exists LabeledLAOFull;
    CREATE table LabeledLAOFull
    AS
      SELECT distinct p.pid,
        p.first, p.middle, p.last,
        "Legislative Analyst Office" AS PersonType,
        year(h.date) AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year,
        lao.state
      FROM Person p
      JOIN LegAnalystOffice lao
      ON p.pid = lao.pid
      JOIN LegAnalystOfficeRepresentation laor
      ON lao.pid = laor.pid
      JOIN Hearing h
      ON laor.hid = h.hid;


    drop table if exists LabeledStateConstFull;
    CREATE table LabeledStateConstFull
    AS
      SELECT distinct p.pid,
        p.first, p.middle, p.last,
        "State Constitutional Office" AS PersonType,
        YEAR(h.date) AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year,
        sa.state
      FROM Person p
      JOIN StateConstOfficeRep sa
      ON p.pid = sa.pid
      JOIN StateConstOfficeRepRepresentation sar
      ON sa.pid = sar.pid
      JOIN Hearing h
      ON sar.hid = h.hid;


    drop table if exists LabeledStateAgencyFull;
    CREATE table LabeledStateAgencyFull
    AS
      SELECT distinct p.pid,
        p.first, p.middle, p.last,
        "State Agency Representative" AS PersonType,
        YEAR(h.date) AS specific_year,
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
      SELECT p.pid,
        p.first, p.middle, p.last,
        "Legislative Staff" AS PersonType,
        sa.state
      FROM Person p
      JOIN LegislativeStaff sa
      ON p.pid = sa.pid;

    drop table if exists SplitTermLop;
    CREATE table SplitTermLop
    AS
      SELECT t1.staff_member as pid,
        year(t1.start_date) as specific_year,
        t1.term_year as session_year
      FROM LegOfficePersonnel t1
      UNION
      SELECT t2.staff_member as pid, IFNULL(YEAR(t2.end_date), year(curdate())) as specific_year,
        t2.term_year as session_year
      FROM LegOfficePersonnel t2;

    alter table SplitTermLop
        add UNIQUE (pid, specific_year, session_year);

    drop table if exists SplitTermOp;
    CREATE TABLE SplitTermOp
    AS
      SELECT distinct t1.staff_member as pid,
        nr.SeqValue                                           AS specific_year,
        IF(nr.SeqValue % 2 = 1, nr.SeqValue, nr.SeqValue - 1) AS session_year
      FROM OfficePersonnel t1
        JOIN NumRange nr
          ON year(t1.start_date) <= nr.SeqValue
             AND (year(t1.end_date) >= nr.SeqValue OR
                  ((t1.end_date is null) and (nr.SeqValue <= year(curdate()))));

    alter table SplitTermOp
      add UNIQUE (pid, specific_year, session_year);

    CREATE OR REPLACE VIEW SplitTermCS
    AS
      SELECT t1.pid, t1.session_year as session_year, YEAR(t1.start_date) as specific_year
      FROM ConsultantServesOn t1
      UNION
      SELECT t2.pid, t2.session_year as session_year, IFNULL(YEAR(t2.end_date), year(curdate())) as specific_year
      FROM ConsultantServesOn t2;

    drop table if exists LegStaffTerms;
    CREATE TABLE LegStaffTerms
    AS
      SELECT pid, specific_year, session_year
      FROM SplitTermLop
      UNION
      SELECT pid, specific_year, session_year
      FROM SplitTermOp
      UNION
      SELECT pid, specific_year, session_year
      FROM SplitTermCS
      UNION
      SELECT lr.pid,
        year(h.date) AS specific_year,
        IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year
      FROM LegislativeStaffRepresentation lr
        JOIN Hearing h
          ON lr.hid = h.hid;

    alter table LegStaffTerms
        add index pid_idx (pid);

    drop table if exists LabeledLegStaffFull;
    CREATE table LabeledLegStaffFull
      AS
      SELECT distinct ls.*, t.specific_year, t.session_year
      FROM LabeledLegStaff ls
      JOIN LegStaffTerms t
      ON ls.pid = t.pid;

    drop table if exists AllPeeps;
    CREATE TABLE AllPeeps
    AS
      SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
      FROM LabeledGenPubFull
      UNION all
      SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
      FROM LabeledLAOFull
      UNION all
      SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
      FROM LabeledLegStaffFull
      UNION all
      SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
      FROM LabeledStateConstFull
      UNION all
      SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
      FROM LabeledStateAgencyFull
      UNION all
      SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
      FROM LabeledLobFull
      UNION all
      SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
      FROM LabeledLegFull;

    alter table AllPeeps
        add INDEX pid_idx (pid);

    DROP table if EXISTS UnlabeledPeople;
    CREATE TABLE UnlabeledPeople
      AS
      SELECT DISTINCT u.pid,
        p.first,
        p.middle,
        p.last,
        'Unlabeled' AS PersonType,
        YEAR(h.date) AS specific_year,
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

    drop table if exists PersonClassificationsTmp;
    CREATE TABLE PersonClassificationsTmp
      AS
      SELECT *, False as is_current
      FROM AllPeeps
      WHERE specific_year > 1980
      UNION all
      SELECT *, False as is_current
      FROM UnlabeledPeople
      WHERE specific_year > 1980;

    alter table PersonClassificationsTmp
      add index pk_idx (pid, PersonType, specific_year, state);

    create table if not exists PersonClassifications (
      pid INTEGER,
      first VARCHAR(255),
      middle VARCHAR(255),
      last VARCHAR (255),
      PersonType VARCHAR(255),
      specific_year YEAR,
      session_year YEAR,
      state VARCHAR(2),
      is_current BOOL DEFAULT FALSE,
      lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
      lastTouched_ts INT(11) AS ((to_seconds(`lastTouched`) - to_seconds('1970-01-01'))),
      dr_id INTEGER UNIQUE AUTO_INCREMENT,

      PRIMARY KEY (pid, PersonType, specific_year, state),
      FOREIGN KEY (pid) REFERENCES Person(pid),
      FOREIGN KEY (state) REFERENCES State(abbrev),

      INDEX session_year_idx (session_year),
      INDEX specific_year_idx (specific_year),
      INDEX pid_idx (pid),
      index person_type_idx (PersonType),
      index state_idx (state),
      index is_current_idx (is_current)
    )
      ENGINE = INNODB
      CHARACTER SET utf8 COLLATE utf8_general_ci;

    insert into PersonClassifications
    (pid, first, middle, last, PersonType, specific_year, session_year, state)
    select tmp.pid, tmp.first, tmp.middle, tmp.last,
      tmp.PersonType, tmp.specific_year, tmp.session_year, tmp.state
    from PersonClassificationsTmp tmp
      left join PersonClassifications og
      on tmp.pid = og.pid
        and tmp.PersonType = og.PersonType
        and tmp.specific_year = og.specific_year
        and tmp.session_year = og.session_year
    where og.pid is null;

    delete pc
    from PersonClassifications pc
      left join PersonClassificationsTmp tmp
      on pc.pid = tmp.pid and pc.PersonType = tmp.PersonType
        and pc.specific_year = tmp.specific_year and tmp.session_year = pc.session_year
    where tmp.pid is null;


    drop table if exists CurrentClass;
    create temporary table CurrentClass
      as
      select pid, max(specific_year) as recent_year
      from PersonClassifications
      group by pid;

    alter table CurrentClass
        add INdex pid_idx (pid);

    update PersonClassifications p
        join CurrentClass c
        on p.pid = c.pid
          and p.specific_year = c.recent_year
      set  is_current = True;


    drop table if EXISTS NumRange;

    DROP VIEW if exists LabeledLeg;
    DROP VIEW if exists SplitTerm;
    DROP VIEW if exists LabeledLegFull;

    DROP VIEW if exists LabeledLobbyist;
    DROP VIEW if exists LobTerms;
    DROP table if exists LabeledLobFull;

    DROP table if exists LabeledGenPubFull;
    DROP table if exists LabeledLAOFull;
    DROP table if exists LabeledStateConstFull;
    DROP table if exists LabeledStateAgencyFull;

    DROP VIEW if exists LabeledLegStaff;
    DROP VIEW if exists SplitTermLop;
    DROP VIEW if exists SplitTermOp;
    DROP VIEW if exists SplitTermCS;
    DROP table if exists LegStaffTerms;
    DROP table if exists LabeledLegStaffFull;

    DROP TABLE if exists AllPeeps;
    DROP table if exists UnlabeledPeople;

    drop table if exists PersonClassificationsTmp;
    drop table if exists CurrentClass;

  END |

DELIMITER ;

