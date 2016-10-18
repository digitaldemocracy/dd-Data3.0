describe PersonClassifications;

select count(*)
from Person;

select count(*)
from Person p
  left join PersonClassifications pc
    on pc.pid = p.pid
where pc.pid is null;

CREATE OR REPLACE VIEW LabeledLeg
AS
  SELECT p.pid,
    p.first, p.middle, p.last,
    "Legislator" AS PersonType,
    l.state
  FROM Person p
  JOIN Legislator l
  ON p.pid = l.pid;

CREATE OR REPLACE VIEW SplitTerm
  AS
SELECT t1.pid, t1.year as session_year, YEAR(t1.start) as specific_year
FROM Term t1
UNION
SELECT t2.pid, t2.year as session_year, YEAR(t2.end) as specific_year
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
    "GeneralPublic" AS PersonType,
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
    "LAO" AS PersonType,
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
    "StateConstOffice" AS PersonType,
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
    "StateAgencyRep" AS PersonType,
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
    "LegStaff" AS PersonType,
    sa.state
  FROM Person p
  JOIN LegislativeStaff sa
  ON p.pid = sa.pid;


CREATE OR REPLACE VIEW SplitTermLop
AS
  SELECT t1.staff_member as pid, YEAR(t1.start_date) as specific_year,
    t1.term_year as session_year
  FROM LegOfficePersonnel t1
  UNION
  SELECT t2.staff_member as pid, YEAR(t2.end_date) as specific_year,
    t2.term_year as session_year
  FROM LegOfficePersonnel t2;

CREATE OR REPLACE VIEW SplitTermOp
AS
  SELECT t1.staff_member as pid, YEAR(t1.start_date) as specific_year,
                 IF(YEAR(t1.start_date) % 2 = 1, YEAR(t1.start_date), YEAR(t1.start_date) - 1) AS session_year
  FROM OfficePersonnel t1
  UNION
  SELECT t2.staff_member as pid, YEAR(t2.end_date) as specific_year,
                 IF(YEAR(t2.end_date) % 2 = 1, YEAR(t2.start_date), YEAR(t2.start_date) - 1) AS session_year
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


CREATE TABLE AllPeeps
AS
  SELECT *
  FROM LabeledGenPubFull
  UNION
  SELECT *
  FROM LabeledLAOFull
  UNION
  SELECT *
  FROM LabeledLegStaffFull
  UNION
  SELECT *
  FROM LabeledStateConstFull
  UNION
  SELECT *
  FROM LabeledStateAgencyFull
  UNION
  SELECT *
  FROM LabeledLobFull
  UNION
  SELECT *
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
  SELECT *
  FROM AllPeeps
  UNION
  SELECT *
  FROM UnlabeledPeople;

DROP VIEW LabeledGenPub;
DROP VIEW LabeledLAO;
DROP VIEW LabeledLegStaff;
DROP VIEW LabeledStateConst;
DROP VIEW LabeledStateAgencyRep;
DROP VIEW LabeledLeg;
DROP VIEW LabeledLobbyist;

-- DROP TABLE LabeledUtterances;
