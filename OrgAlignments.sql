DROP VIEW IF EXISTS AllProfs;
DROP VIEW IF EXISTS OrgAlignmentsUtter;
DROP VIEW IF EXISTS OrgAlignmentsDistinct;
DROP VIEW IF EXISTS OrgAlignmentsDefin;
DROP VIEW IF EXISTS OrgAlignmentsTrumped;
DROP VIEW IF EXISTS OrgAlignmentsUnknown;
DROP VIEW IF EXISTS OrgAlignmentsMulti;
DROP VIEW IF EXISTS OrgAMUtter;
DROP VIEW IF EXISTS OrgAlignmentsExtra;

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
CREATE VIEW OrgAlignmentsUtter
AS
SELECT ap.oid, bd.bid, u.uid, u.alignment
FROM AllProfs ap  
    JOIN currentUtterance u 
    ON ap.pid = u.pid
        AND ap.did = u.did
    JOIN BillDiscussion bd
    ON u.did = bd.did
WHERE ap.oid IS NOT NULL;

-- Uses group by to get all all the distinct alignments of 
-- an organization associated with the specific bill
CREATE VIEW OrgAlignmentsDistinct
AS 
SELECT oid, bid, alignment
FROM OrgAlignmentsUtter
GROUP BY oid, bid, alignment;

-- Gets all the alignments we are certain about
CREATE VIEW OrgAlignmentsDefin
AS
SELECT oid, bid, alignment 
FROM OrgAlignmentsDistinct
WHERE alignment != 'NA'
GROUP BY oid, bid 
HAVING COUNT(*) = 1;

-- Next gets all cases where the indeterminate is
-- trumped by a clear position
CREATE VIEW OrgAlignmentsTrumped
AS 
SELECT oid, bid, alignment 
FROM OrgAlignmentsDistinct
WHERE (oid, bid) IN (SELECT oid, bid
                     FROM OrgAlignmentsDistinct
                     GROUP BY oid, bid 
                     HAVING COUNT(*) = 2)
    AND (oid, bid) IN (SELECT oid, bid
                       FROM OrgAlignmentsDistinct
                       WHERE alignment = 'Indeterminate'
                         OR alignment = 'Neutral')
    AND alignment != 'Indeterminate' 
    AND alignment != 'Neutral';

-- Gets all the profiles that contain both for 
-- and against. These are left unknown
CREATE VIEW OrgAlignmentsUnknown
AS 
SELECT oid, bid, 'Unknown' AS alignment
FROM OrgAlignmentsDistinct
WHERE (oid, bid) IN (SELECT oid, bid
                       FROM OrgAlignmentsDistinct
                       WHERE alignment = 'Against')
    AND alignment = 'For';

-- Rounds up all the left over combos with multiple
-- alignments
CREATE VIEW OrgAlignmentsMulti
AS 
SELECT oid, bid
FROM OrgAlignmentsDistinct
WHERE (oid, bid) IN (SELECT oid, bid
                     FROM OrgAlignmentsDistinct
                     GROUP BY oid, bid
                     HAVING COUNT(*) > 1)
    AND (oid, bid) NOT IN (SELECT oid, bid 
                           FROM OrgAlignmentsTrumped
                           UNION 
                           SELECT oid, bid
                           FROM OrgAlignmentsUnknown);

-- Binds the multi alignments to their utterances. You
-- grab the latest valued one
CREATE VIEW OrgAMUtter
AS 
SELECT oid, bid, MAX(uid) AS uid
FROM OrgAlignmentsUtter 
WHERE (oid, bid) IN (SELECT oid, bid
                     FROM OrgAlignmentsMulti)
GROUP BY oid, bid;

-- Gets the alignment of that highest utterance
CREATE VIEW OrgAlignmentsExtra
AS 
SELECT oamu.oid, oamu.bid, u.alignment 
FROM OrgAMUtter oamu 
    JOIN currentUtterance u 
    ON oamu.uid = u.uid;

-- Gets all the alignments neatly into a table. This is what
-- Toshi sees
DROP TABLE IF EXISTS OrgAlignments;
CREATE TABLE OrgAlignments 
AS
    SELECT oid, bid, alignment 
    FROM OrgAlignmentsDefin 
UNION 
    SELECT oid, bid, alignment 
    FROM OrgAlignmentsTrumped
UNION 
    SELECT oid, bid, alignment 
    FROM OrgAlignmentsExtra;

 
DROP VIEW IF EXISTS AllProfs;
DROP VIEW IF EXISTS OrgAlignmentsUtter;
DROP VIEW IF EXISTS OrgAlignmentsDistinct;
DROP VIEW IF EXISTS OrgAlignmentsDefin;
DROP VIEW IF EXISTS OrgAlignmentsTrumped;
DROP VIEW IF EXISTS OrgAlignmentsUnknown;
DROP VIEW IF EXISTS OrgAlignmentsMulti;
DROP VIEW IF EXISTS OrgAMUtter;
DROP VIEW IF EXISTS OrgAlignmentsExtra;
