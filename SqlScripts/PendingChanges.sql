ALTER TABLE Bill
    ADD visibility_flag BOOLEAN DEFAULT 1;


-- Updating the Term to have Bio
ALTER TABLE Term
    ADD official_bio TEXT;

UPDATE Term as t
    join Legislator l
    on t.pid = l.pid
set t.official_bio = l.OfficialBio;

alter table Legislator
    drop column OfficialBio;


# Updates to Bill
ALTER TABLE Bill
    ADD year YEAR;

update Bill
    set year = SessionYear;

update Bill
    set SessionYear = 2015
where SessionYear is not null;


# Initial utterance stuff
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
where pid is not null and did is not null
  and did > -2
group by pid, did;

# alter table Utterance
#   drop first_utterance_flag;

CREATE OR REPLACE VIEW currentUtterance
AS SELECT uid, vid, pid, time, endTime, text, type, alignment, state, did,
     lastTouched
   FROM Utterance
   WHERE current = TRUE AND finalized = TRUE ORDER BY time DESC;

# Unioning rep tables
drop table CombinedRepresentations;
create table CombinedRepresentations
  as
  select pid, h.hid, did, oid, gp.state, year(h.date) as year
  from GeneralPublic gp
    join Hearing h
    on gp.hid = h.hid
  union
  select pid, h.hid, did, oid, lr.state, year(h.date) as year
  from LobbyistRepresentation lr
    join Hearing h
    on lr.hid = h.hid;

alter table CombinedRepresentations
    add dr_id INT UNIQUE AUTO_INCREMENT;

# Adding source field to Organizations
alter table Organizations
    add source VARCHAR(255);

# Lobbyist Employment view for Kristian
CREATE TABLE CombinedLobbyistEmployers
  AS
SELECT le.pid,
  lf.filer_naml as assoc_name,
  le.rpt_date,
  le.rpt_date_ts,
  le.ls_beg_yr,
  le.ls_end_yr,
  le.state
FROM LobbyistEmployment le
  JOIN LobbyingFirmState lf
  ON le.sender_id = lf.filer_id
    AND le.state = lf.state
UNION
SELECT le.pid,
  o.name as assoc_name,
  le.rpt_date,
  le.rpt_date_ts,
  le.ls_beg_yr,
  le.ls_end_yr,
  le.state
FROM LobbyistDirectEmployment le
  JOIN Organizations o
    ON le.lobbyist_employer = o.oid;

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



# Known clients view for Kristian based on Lobbying Contracts
drop table KnownClients;
CREATE TABLE KnownClients
  AS
select distinct le.pid,
      o.name as assoc_name,
      o.oid,
      year(lc.rpt_date) as year,
      le.state
from LobbyistEmployment le
  join LobbyingContracts lc
  on lc.filer_id = le.sender_id
    and lc.state = le.state
  join Organizations o
  on lc.lobbyist_employer = o.oid;

# Sessions table
CREATE TABLE IF NOT EXISTS Session (
  state VARCHAR(2),
  start_year YEAR,
  end_year YEAR,
  lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),
  lastTouched_ts INT(11) AS (TO_SECONDS(lastTouched) - TO_SECONDS('1970-01-01')),

  PRIMARY KEY (state, start_year, end_year),
  FOREIGN KEY (state) REFERENCES State(abbrev)
)
  ENGINE = INNODB
  CHARACTER SET utf8 COLLATE utf8_general_ci;

insert into Session
(state, start_year, end_year)
values
  ('CA', 2001, 2002),
  ('CA', 2003, 2004),
  ('CA', 2005, 2006),
  ('CA', 2007, 2008),
  ('CA', 2009, 2010),
  ('CA', 2011, 2012),
  ('CA', 2013, 2014),
  ('CA', 2015, 2016),
  ('CA', 2017, 2018),
  ('NY', 2001, 2002),
  ('NY', 2003, 2004),
  ('NY', 2005, 2006),
  ('NY', 2007, 2008),
  ('NY', 2009, 2010),
  ('NY', 2011, 2012),
  ('NY', 2013, 2014),
  ('NY', 2015, 2016),
  ('NY', 2017, 2018);


alter table KnownClients
    add UNIQUE (pid, assoc_name, oid, year, state);


alter table Behests
  add Index sessionYear (sessionYear);

alter table Gift
  add Index session_year_idx (sessionYear);

alter table GiftCombined
  add Index session_year_idx (sessionYear);

alter table Hearing
  add Index session_year_idx (session_year);

alter table CombinedRepresentations
  add Index year_idx (year);

alter table LobbyistE
  add Index session_year_idx (session_year);

alter table PersonClassifications
    add index pid_idx (pid);

alter table CombinedRepresentations
  add index pid_idx (pid);

alter table KnownClients
  add index pid_idx (pid);

alter table GiftCombined
  add index oid_idx (oid);

alter table Behests
  add index oid_idx (payee);

alter table CombinedRepresentations
  add index hid_idx (hid);

alter table CombinedRepresentations
  add index did_idx (did);

alter table CombinedRepresentations
  add index oid_idx (oid);

alter table KnownClients
  add index oid_idx (oid);

alter table Hearing
  add index date_idx (date);

alter table Organizations
  add index name_idx (name);
