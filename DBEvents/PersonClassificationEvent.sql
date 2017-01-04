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
SELECT t2.pid, t2.year as session_year, IFNULL(YEAR(t2.end), 2016) as specific_year
FROM Term t2;

CREATE OR REPLACE VIEW LabeledLegFull
  AS
SELECT l.*,
  st.specific_year,
  st.session_year
FROM LabeledLeg l
  JOIN SplitTerm st
  ON l.pid = st.pid;

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
SELECT pid,
  year(rpt_date) AS specific_year,
  IF(YEAR(rpt_date) % 2 = 1, YEAR(rpt_date), YEAR(rpt_date) - 1) AS session_year
FROM LobbyistEmployment
  UNION
SELECT pid,
  year(rpt_date) AS specific_year,
  IF(YEAR(rpt_date) % 2 = 1, YEAR(rpt_date), YEAR(rpt_date) - 1) AS session_year
FROM LobbyistDirectEmployment
  UNION
SELECT lr.pid,
  year(h.date) AS specific_year,
  IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year
FROM LobbyistRepresentation lr
    JOIN Hearing h
    ON lr.hid = h.hid;

CREATE OR REPLACE VIEW LabeledLobFull
AS
  SELECT l.*,
    lt.specific_year,
    lt.session_year
  FROM LabeledLobbyist l
    JOIN LobTerms lt
      ON l.pid = lt.pid;

CREATE OR REPLACE VIEW LabeledGenPubFull
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

CREATE OR REPLACE VIEW LabeledLAOFull
AS
  SELECT p.pid,
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


CREATE OR REPLACE VIEW LabeledStateConstFull
AS
  SELECT p.pid,
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


CREATE OR REPLACE VIEW LabeledStateAgencyFull
AS
  SELECT p.pid,
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


CREATE OR REPLACE VIEW SplitTermLop
AS
  SELECT t1.staff_member as pid, GREATEST(YEAR(t1.start_date), 1999) as specific_year,
    t1.term_year as session_year
  FROM LegOfficePersonnel t1
  UNION
  SELECT t2.staff_member as pid, IFNULL(YEAR(t2.end_date), 2016) as specific_year,
    t2.term_year as session_year
  FROM LegOfficePersonnel t2;

CREATE OR REPLACE VIEW SplitTermOp
AS
  SELECT t1.staff_member as pid, GREATEST(YEAR(t1.start_date), 1999) as specific_year,
                 GREATEST(IF(YEAR(t1.start_date) % 2 = 1, YEAR(t1.start_date), YEAR(t1.start_date) - 1), 1999) AS session_year
  FROM OfficePersonnel t1
  UNION
  SELECT t2.staff_member as pid, IFNULL(YEAR(t2.end_date), 2016) as specific_year,
                 GREATEST(IF(YEAR(t2.end_date) % 2 = 1, YEAR(t2.end_date), YEAR(t2.end_date) - 1), 1999) AS session_year
  FROM OfficePersonnel t2;


CREATE OR REPLACE VIEW LegStaffTerms
AS
  SELECT *
  FROM SplitTermLop
  UNION
  SELECT *
  FROM SplitTermOp
  UNION
  SELECT lr.pid,
    year(h.date) AS specific_year,
    IF(YEAR(h.date) % 2 = 1, YEAR(h.date), YEAR(h.date) - 1) AS session_year
  FROM LobbyistRepresentation lr
    JOIN Hearing h
      ON lr.hid = h.hid;


CREATE OR REPLACE VIEW LabeledLegStaffFull
  AS
  SELECT ls.*, t.specific_year, t.session_year
  FROM LabeledLegStaff ls
  JOIN LegStaffTerms t
  ON ls.pid = t.pid;


drop table AllPeeps;
CREATE TABLE AllPeeps
AS
  SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
  FROM LabeledGenPubFull
  UNION
  SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
  FROM LabeledLAOFull
  UNION
  SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
  FROM LabeledLegStaffFull
  UNION
  SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
  FROM LabeledStateConstFull
  UNION
  SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
  FROM LabeledStateAgencyFull
  UNION
  SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
  FROM LabeledLobFull
  UNION
  SELECT pid, first, middle, last, PersonType, specific_year, session_year, state
  FROM LabeledLegFull;

CREATE OR REPLACE VIEW UnlabeledPeople
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


drop table PersonClassifications;
CREATE TABLE PersonClassifications
  AS
  SELECT *, False as is_current
  FROM AllPeeps
  UNION
  SELECT *, False as is_current
  FROM UnlabeledPeople;

alter table PersonClassifications
  add Index session_year_idx (session_year);

alter table PersonClassifications
  add Index specific_year_idx (specific_year);

alter table PersonClassifications
  add Index pid_idx (pid);

drop table if exists CurrentClass;
create temporary table CurrentClass
  as
  select pid, max(specific_year) as recent_year
  from PersonClassifications
  group by pid;

update PersonClassifications p
    join CurrentClass c
    on p.pid = c.pid
      and p.specific_year = c.recent_year
  set  is_current = True;

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

create view YearRanges
  as
select pid, PersonType, min(specific_year) as low, max(specific_year) as high
from PersonClassifications pc
group by pid, PersonType;

insert into PersonClassifications
(pid, first, middle, last, PersonType, specific_year, session_year, state, is_current)
select p.pid, p.first, p.middle, p.last, yr.PersonType,
  n.SeqValue as specific_year,
  IF(n.SeqValue % 2 = 1, n.SeqValue, n.SeqValue - 1) AS session_year,
  'CA' as state,
  False as is_current
from YearRanges yr
  join NumRange n
  on yr.low < n.SeqValue
    and yr.high > n.SeqValue
  join Person p
    on yr.pid = p.pid
  left join PersonClassifications pc
    on yr.pid = pc.pid
      and yr.PersonType = pc.PersonType
      and n.SeqValue = pc.specific_year
where pc.pid is null;


alter table PersonClassifications
  add dr_id INT AUTO_INCREMENT UNIQUE ;

alter table PersonClassifications
    add index person_type_idx (PersonType),
    add index state_idx (state),
    add index is_current_idx (is_current);


DROP VIEW LabeledGenPubFull;
DROP VIEW LabeledLAOFull;
DROP VIEW LabeledLegStaff;
DROP VIEW SplitTermLop;
DROP VIEW SplitTermOp;
DROP VIEW LegStaffTerms;
DROP VIEW LabeledLegStaffFull;
DROP VIEW LabeledStateConstFull;
DROP VIEW LabeledStateAgencyFull;
DROP VIEW LabeledLeg;
DROP VIEW SplitTerm;
DROP VIEW LabeledLegFull;
DROP VIEW LabeledLobbyist;
DROP VIEW LobTerms;
DROP VIEW LabeledLobFull;
DROP TABLE AllPeeps;
DROP VIEW UnlabeledPeople;

alter table PersonClassifications

-- DROP TABLE LabeledUtterances;
