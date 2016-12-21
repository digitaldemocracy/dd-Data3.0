DROP VIEW IF EXISTS UtterInfo;
DROP VIEW IF EXISTS LegDidWord;
DROP VIEW IF EXISTS DidWord;
DROP VIEW IF EXISTS LegDidWordPer;
DROP VIEW IF EXISTS LegHidWord;
DROP VIEW IF EXISTS HearingWord;
DROP VIEW IF EXISTS LegHidWordPer;
DROP VIEW IF EXISTS LegWordAvg;
DROP VIEW IF EXISTS TalkingLegs;
DROP VIEW IF EXISTS ProperLegs;
DROP VIEW IF EXISTS AllLegs;
DROP VIEW IF EXISTS AllLegsInfo;
DROP TABLE IF EXISTS LegParticipation;
DROP VIEW IF EXISTS LegHearingParticipation;
DROP TABLE IF EXISTS LegAvgPercentParticipation;

-- Creates view with the word counts for every utterance
CREATE VIEW UtterInfo
AS 
SELECT u.uid, u.pid, v.hid, u.did, GetWordCount(u.text)
    AS WordCount, u.endTime - u.time AS Time
FROM currentUtterance u
    JOIN Video v
    ON u.vid = v.vid
    JOIN Legislator l
    ON l.pid = u.pid;

-- Gives the word counts for each legislator based on a
-- hearing. Also their percentages talking.
CREATE VIEW LegDidWord
AS
SELECT u.did, u.pid, SUM(u.WordCount) AS 
    WordCount, SUM(u.Time) AS Time
FROM UtterInfo u
WHERE u.did IS NOT NULL
GROUP BY u.pid, u.did;

-- Gives the word count for a bill discussion
-- using only legislators
CREATE VIEW DidWord
AS 
SELECT did, SUM(WordCount) AS WordCount, SUM(Time)
    AS Time
FROM LegDidWord
WHERE did IS NOT NULL
GROUP BY did;

-- give the percentage talking as well
CREATE VIEW LegDidWordPer
AS
SELECT ldw.did, ldw.pid, ldw.WordCount, ldw.Time,
    ldw.WordCount / dw.WordCount AS PercentCount,
    ldw.Time / dw.Time AS PercentTime
FROM LegDidWord ldw 
    JOIN DidWord dw 
    ON ldw.did = dw.did;


-- Gets the word count for every legislator 
-- at a hearing
CREATE VIEW LegHidWord
AS
SELECT u.pid, u.hid, SUM(u.WordCount) AS WordCount,
    SUM(u.Time) AS Time
FROM UtterInfo u
GROUP BY u.pid, u.hid;  

-- Gets the word count for the hearing given only
-- the legislators
CREATE VIEW HearingWord
AS
SELECT hid, SUM(WordCount) AS WordCount,
    SUM(Time) AS Time 
FROM LegHidWord
GROUP BY hid;

-- gives the percentage as well
CREATE VIEW LegHidWordPer
AS
SELECT lhw.hid, lhw.pid, lhw.WordCount, lhw.Time,
    lhw.WordCount / hw.WordCount AS PercentCount,
    lhw.Time / hw.Time AS PercentTime
FROM LegHidWord lhw 
    JOIN HearingWord hw 
    ON lhw.hid = hw.hid;

-- Gets the average number of words a legislator speaks
-- at a hearing
CREATE VIEW LegWordAvg
AS 
SELECT pid, SUM(WordCount) / COUNT(*) AS AvgWords
FROM LegHidWord 
GROUP BY pid;

-- All the legislators that should be a specific hearings
CREATE VIEW ProperLegs
AS 
SELECT DISTINCT ch.hid, s.pid
FROM CommitteeHearings ch
    JOIN servesOn s
    ON s.cid = ch.cid;

-- All the legislators that talked at specific hearings
CREATE VIEW TalkingLegs
AS
SELECT DISTINCT hid, pid
FROM UtterInfo;

-- Combines the prior two tables to get all the legs at a 
-- hearing, as well as all the legs that should have been
-- there
CREATE VIEW AllLegs
AS 
    (SELECT hid, pid
    FROM ProperLegs)
UNION
    (SELECT hid, pid
    FROM TalkingLegs);

-- Collects everybody who should be present at a given 
-- did
CREATE VIEW AllLegsInfo
AS
SELECT DISTINCT al.hid, bd.did, bd.bid, al.pid, p.first, p.last,
    t.party
FROM AllLegs al
    LEFT JOIN BillDiscussion bd 
    ON bd.hid = al.hid 
    JOIN Person p
    ON al.pid = p.pid
    JOIN Term t 
    ON p.pid = t.pid;

-- Creates the necessary view for Leg participation
CREATE TABLE LegParticipation
AS
SELECT al.hid, al.did, al.bid, al.pid, al.first, al.last,
    al.party,
    IFNULL(ldw.WordCount, 0) AS LegBillWordCount,
    IFNULL(ldw.Time, 0) AS LegBillTime,
    IFNULL(ldw.PercentCount, 0) AS LegBillPercentWord,
    IFNULL(ldw.PercentTime, 0) AS LegBillPercentTime,
    IFNULL(lhw.WordCount, 0) AS LegHearingWordCount,
    IFNULL(lhw.Time, 0) AS LegHearingTime,
    IFNULL(lhw.PercentCount, 0) AS LegHearingPercentWord,
    IFNULL(lhw.PercentTime, 0) AS LegHearingPercentTime,
    IFNULL(lwa.AvgWords, 0) AS LegHearingAvg,
    IFNULL(bdw.WordCount, 0) AS BillWordCount,
    IFNULL(hw.WordCount, 0) AS HearingWordCount
FROM AllLegsInfo al 
    LEFT JOIN LegDidWordPer ldw
    ON al.pid = ldw.pid
        AND al.did = ldw.did
    LEFT JOIN LegHidWordPer lhw 
    ON al.pid = lhw.pid
        AND al.hid = lhw.hid
    LEFT JOIN LegWordAvg lwa 
    ON al.pid = lwa.pid
    LEFT JOIN DidWord bdw 
    ON al.did = bdw.did
    LEFT JOIN HearingWord hw 
    ON al.hid = hw.hid;

-- Used to help you get the Avg Participation
CREATE VIEW LegHearingParticipation
AS
SELECT DISTINCT pid, hid, first, last, LegHearingWordCount,
    HearingWordCount
FROM LegParticipation;


-- Gets the average percent participation for each leg
CREATE TABLE LegAvgPercentParticipation
AS
SELECT pid, first, last, SUM(LegHearingWordCount) / 
    SUM(HearingWordCount) AS AvgPercentParticipation
FROM LegParticipation
GROUP BY pid;

    
DROP VIEW UtterInfo;
DROP VIEW LegDidWord;
DROP VIEW DidWord;
DROP VIEW LegDidWordPer;
DROP VIEW LegHidWord;
DROP VIEW HearingWord;
DROP VIEW LegHidWordPer;
DROP VIEW LegWordAvg;
DROP VIEW TalkingLegs;
DROP VIEW ProperLegs;
DROP VIEW AllLegs;
DROP VIEW AllLegsInfo;
DROP VIEW LegHearingParticipation;