DROP EVENT IF EXISTS OrgAlignments;
delimiter |

CREATE EVENT OrgAlignments
  ON SCHEDULE
    EVERY 1 DAY STARTS '2016-09-21 07:10:00'
DO
  BEGIN

    DROP VIEW IF EXISTS AllProfs;
    DROP table IF EXISTS OrgAlignmentsUtter;
    DROP table IF EXISTS OrgAlignmentsDistinct;
    DROP table IF EXISTS OrgAlignmentsDefin;
    DROP table IF EXISTS OrgAlignmentsTrumped;
    DROP VIEW IF EXISTS OrgAlignmentsUnknown;
    DROP VIEW IF EXISTS OrgAlignmentsMulti;
    DROP VIEW IF EXISTS OrgAMUtter;
    DROP table IF EXISTS OrgAlignmentsExtra;
    DROP TABLE if exists OrgAlignmentsTmp;

    -- Gives you the alignments of organizaitons on bills,
    -- based on the alignments of the lobbyists supporting them
    CREATE VIEW AllProfs
    AS
      SELECT pid, oid, did
      FROM LobbyistRepresentation
      UNION
      SELECT pid, oid, did
      FROM GeneralPublic;

    -- Binds utterances to every organization for each bill.
    -- It's set up so that it is almost like the organizations
    -- themselves said these things
    CREATE table OrgAlignmentsUtter
    AS
      SELECT ap.oid, bd.bid, u.uid, u.alignment, bd.hid
      FROM AllProfs ap
        JOIN currentUtterance u
          ON ap.pid = u.pid
             AND ap.did = u.did
        JOIN BillDiscussion bd
          ON u.did = bd.did
      WHERE ap.oid IS NOT NULL
        and bd.bid not like '%NO BILL%';

    alter table OrgAlignmentsUtter
      add key (oid, bid, alignment, hid);

    -- Uses group by to get all all the distinct alignments of
    -- an organization associated with the specific bill, at a
    -- specific hearing
    CREATE table OrgAlignmentsDistinct
    AS
      SELECT distinct oid, bid, alignment, hid
      FROM OrgAlignmentsUtter;

    alter table OrgAlignmentsDistinct
      add key (oid, bid, alignment, hid);

    -- Gets all the alignments we are certain about
    CREATE temporary table OrgAlignmentsDefin
    AS
      SELECT oa.oid, oa.bid, oa.alignment, oa.hid, h.date as alignment_date
      FROM OrgAlignmentsDistinct oa
        join Hearing h
        on oa.hid = h.hid
      WHERE alignment != 'NA'
      GROUP BY oid, bid, hid
      HAVING COUNT(*) = 1;

    alter table OrgAlignmentsDefin
      add key(oid, bid, hid, alignment);

    -- Next gets all cases where the indeterminate is
    -- trumped by a clear position
    CREATE table OrgAlignmentsTrumped
    AS
      SELECT oa.oid, oa.bid, oa.alignment, oa.hid, h.date as alignment_date
      FROM OrgAlignmentsDistinct oa
        join Hearing h
          on oa.hid = h.hid
      WHERE (oa.oid, oa.bid, oa.hid) IN (SELECT oid, bid, hid
                                FROM OrgAlignmentsDistinct
                                GROUP BY oid, bid, hid
                                HAVING COUNT(*) = 2)
            AND alignment != 'Indeterminate'
            AND alignment != 'Neutral';

    alter table OrgAlignmentsTrumped
      add key(oid, bid, hid, alignment);

    -- Gets all the profiles that contain both for
    -- and against. These are left unknown
    CREATE VIEW OrgAlignmentsUnknown
    AS
      SELECT oid, bid, 'Unknown' AS alignment, hid
      FROM OrgAlignmentsDistinct
      WHERE (oid, bid, hid) IN (SELECT oid, bid, hid
                                FROM OrgAlignmentsDistinct
                                WHERE alignment = 'Against')
            AND alignment = 'For';

    -- Rounds up all the left over combos with multiple
    -- alignments
    CREATE VIEW OrgAlignmentsMulti
    AS
      SELECT oid, bid, hid
      FROM OrgAlignmentsDistinct
      WHERE (oid, bid, hid) IN (SELECT oid, bid, hid
                                FROM OrgAlignmentsDistinct
                                GROUP BY oid, bid, hid
                                HAVING COUNT(*) > 1)
            AND (oid, bid, hid) NOT IN (SELECT oid, bid, hid
                                        FROM OrgAlignmentsTrumped
                                        UNION
                                        SELECT oid, bid, hid
                                        FROM OrgAlignmentsUnknown);

    -- Binds the multi alignments to their utterances. You
    -- grab the latest valued one. Indeterminates are ignored
    CREATE VIEW OrgAMUtter
    AS
      SELECT oid, bid, hid, MAX(uid) AS uid
      FROM OrgAlignmentsUtter
      WHERE (oid, bid, hid) IN (SELECT oid, bid, hid
                                FROM OrgAlignmentsMulti
                                WHERE alignment != "Indeterminate")
      GROUP BY oid, bid, hid;

    -- Gets the alignment of that highest utterance
    CREATE temporary table OrgAlignmentsExtra
    AS
      SELECT oamu.oid, oamu.bid, oamu.hid, u.alignment, h.date as alignment_date
      FROM OrgAMUtter oamu
        JOIN currentUtterance u
          ON oamu.uid = u.uid
        join Hearing h
          on oamu.hid = h.hid;

    alter table OrgAlignmentsExtra
      add key (oid, bid, hid, alignment);

    CREATE TEMPORARY TABLE OrgAlignmentsTmp
      as
      SELECT t.*, 0
      FROM
        (SELECT oid, bid, hid, alignment, alignment_date
         FROM OrgAlignmentsDefin
         UNION
         SELECT oid, bid, hid, alignment, alignment_date
         FROM OrgAlignmentsTrumped
         UNION
         SELECT oid, bid, hid, alignment, alignment_date
         FROM OrgAlignmentsExtra) t;

    alter table OrgAlignmentsTmp
      add key(oid, bid, hid, alignment);

    -- Gets all the alignments neatly into a table. This is what
    -- Toshi sees
    INSERT INTO OrgAlignments
    (oid, bid, hid, alignment, alignment_date, analysis_flag)
      SELECT t.*, 0
      FROM
        (SELECT oid, bid, hid, alignment, alignment_date
         FROM OrgAlignmentsDefin
         UNION
         SELECT oid, bid, hid, alignment, alignment_date
         FROM OrgAlignmentsTrumped
         UNION
         SELECT oid, bid, hid, alignment, alignment_date
         FROM OrgAlignmentsExtra) t
      WHERE (t.oid, t.bid, t.hid, t.alignment) NOT IN (SELECT oid, bid, hid, alignment
                                                       FROM OrgAlignments
                                                       WHERE analysis_flag = 0);

    update OrgAlignments oa
        join Bill b
          on oa.bid = b.bid
      set session_year = b.sessionYear;

    DROP VIEW IF EXISTS AllProfs;
    DROP table IF EXISTS OrgAlignmentsUtter;
    DROP table IF EXISTS OrgAlignmentsDistinct;
    DROP table IF EXISTS OrgAlignmentsDefin;
    DROP table IF EXISTS OrgAlignmentsTrumped;
    DROP VIEW IF EXISTS OrgAlignmentsUnknown;
    DROP VIEW IF EXISTS OrgAlignmentsMulti;
    DROP VIEW IF EXISTS OrgAMUtter;
    DROP table IF EXISTS OrgAlignmentsExtra;
    DROP TABLE if exists OrgAlignmentsTmp;

  END |

delimiter ;
