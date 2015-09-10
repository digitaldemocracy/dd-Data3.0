DROP VIEW IF EXISTS AllProfs;
DROP VIEW IF EXISTS OrgAlignmentsUtter;
DROP VIEW IF EXISTS OrgAlignmentsUtterLatest;
DROP TABLE IF EXISTS OrgAlignments;

-- Gives you the alignments of organizaitons on bills, 
-- based on the alignments of the lobbyists supporting them
CREATE VIEW AllProfs
AS
    SELECT pid, oid, did
    FROM LobbyistRepresentation
UNION
    SELECT pid, oid, did
    FROM GeneralPublic;

CREATE VIEW OrgAlignmentsUtter
AS
SELECT ap.pid, ap.oid, ap.did, MAX(u.uid) AS uid
FROM AllProfs ap  
    JOIN currentUtterance u 
    ON ap.pid = u.pid
        AND ap.did = u.did
GROUP BY ap.pid, ap.oid, ap.did;

CREATE VIEW OrgAlignmentsUtterLatest
AS
SELECT oau.oid, bd.bid, MAX(oau.uid) AS uid
FROM OrgAlignmentsUtter oau
    JOIN BillDiscussion bd 
    ON oau.did = bd.did
GROUP BY oau.oid, bd.bid;

CREATE TABLE OrgAlignments
AS
SELECT DISTINCT oau.bid, oau.oid, o.name, u.alignment 
FROM OrgAlignmentsUtterLatest oau
    JOIN currentUtterance u 
    ON oau.uid = u.uid
    JOIN Organizations o 
    ON oau.oid = o.oid;

DROP VIEW AllProfs;
DROP VIEW OrgAlignmentsUtter;
DROP VIEW OrgAlignmentsUtterLatest;