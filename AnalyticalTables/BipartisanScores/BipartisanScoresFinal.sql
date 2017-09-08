

-- All votes  with "do pass" in the motion text
CREATE OR REPLACE VIEW DoPassVotes
AS
  SELECT bid,
    voteId,
    m.mid,
    b.cid,
    VoteDate,
    ayes,
    naes,
    abstain,
    result,
    c.house,
    c.type
  FROM BillVoteSummary b
    JOIN Motion m
      ON b.mid = m.mid
    JOIN Committee c
      ON b.cid = c.cid
  WHERE m.doPass = 1;

-- All votes which we consider 'passing' on the floor
CREATE OR REPLACE VIEW FloorVotes
AS
  SELECT b.bid,
    b.voteId,
    b.mid,
    b.cid,
    b.VoteDate,
    b.ayes,
    b.naes,
    b.abstain,
    b.result,
    c.house,
    c.type
  FROM BillVoteSummary b
    JOIN Committee c
      ON b.cid = c.cid
    JOIN Motion m
      on b.mid = m.mid
  WHERE m.text like '%reading%';

-- *All* votes which we consider 'passing'. These are what we want to measure with
-- Also note that this looks slightly different than how "passing votes" looks for
-- the alignment meter
CREATE OR REPLACE VIEW AllPassingVotesTmp
AS
  SELECT bvd.pid,
         v.bid,
         v.voteId,
         v.mid,
         v.cid,
         v.VoteDate,
         v.result,
         v.house,
         v.type,
         IF(YEAR(v.VoteDate) % 2 = 0, YEAR(v.VoteDate) - 1, YEAR(v.VoteDate)) as session_year,
    naes = 0 or ayes = 0 as unanimous,
      bid like '%ACR%' or bid like '%SCR%' or bid like '%HR%' or bid like '%SR%' or bid like '%AJR%'
  or bid like '%SJR%' as resolution,
    IF(bvd.result = 'ABS', 1, 0) as abstain_vote
  FROM DoPassVotes v
    join BillVoteDetail bvd
      on v.voteId = bvd.voteId
  WHERE bid like 'CA%'
  UNION
  SELECT bvd.pid,
    v.bid,
    v.voteId,
    v.mid,
    v.cid,
    v.VoteDate,
    v.result,
    v.house,
    v.type,
    IF(YEAR(v.VoteDate) % 2 = 0, YEAR(v.VoteDate) - 1, YEAR(v.VoteDate)) as session_year,
    naes = 0 or ayes = 0 as unanimous,
    bid like '%ACR%' or bid like '%SCR%' or bid like '%HR%' or bid like '%SR%' or bid like '%AJR%'
    or bid like '%SJR%' as resolution,
  IF(bvd.result = 'ABS', 1, 0) as abstain_vote
  FROM FloorVotes v
    join BillVoteDetail bvd
      on v.voteId = bvd.voteId
  WHERE bid like 'CA%';

drop view AllPassingVotes;
-- Separated just so the join to party is marginally easier
DROP TABLE IF EXISTS AllPassingVotes;
CREATE TABLE AllPassingVotes
  AS
SELECT DISTINCT v.*, t.party
FROM AllPassingVotesTmp v
    JOIN Term t
    ON v.pid = t.pid
      AND v.session_year = t.year;

ALTER TABLE AllPassingVotes
    ADD KEY (pid, voteId);

CREATE OR REPLACE VIEW DummyVoteOptions
AS
  SELECT 'Republican' AS party, 'AYE' AS vote
  UNION
  SELECT 'Republican' AS party, 'NOE' AS vote
  UNION
  SELECT 'Democrat' AS party, 'AYE' AS vote
  UNION
  SELECT 'Democrat' AS party, 'NOE' AS vote;

-- This is kinda weird, by I needed to get every vote associated with every possible vote
-- option
CREATE OR REPLACE VIEW DummyVotes
AS
  SELECT *
  FROM NonUniVotes
    JOIN DummyVoteOptions;

CREATE OR REPLACE VIEW VotesByParty
AS
  SELECT v.voteId,
    t.party,
#     t.year,
    det.result,
    COUNT(DISTINCT t.pid) AS votes
  FROM NonUniVotes v
    JOIN BillVoteDetail det
      ON v.voteId = det.voteId
    JOIN Term t
      ON det.pid = t.pid
#   WHERE t.year = 2015
#   GROUP BY v.voteId, t.Party, det.result, t.year;
  GROUP BY v.voteId, t.Party, det.result;

-- joined to DummyVotes to account for all voting options
CREATE OR REPLACE VIEW VotesByPartyAll
AS
  SELECT d.voteId,
    d.party,
#     v.year,
    d.vote AS result,
    IFNULL(v.votes, 0) AS votes
  FROM DummyVotes d
    LEFT JOIN VotesByParty v
      ON d.voteId = v.voteId
         AND v.party = d.party
         AND v.result = d.vote;


DROP TABLE IF EXISTS VotesByPartyJoined;
CREATE TABLE VotesByPartyJoined
AS
  SELECT v1.voteId,
    v1.party,
#     v1.year,
    v1.votes AS 'AYES',
    v2.votes AS 'NAES',
    CASE
    WHEN v1.votes > v2.votes THEN 'AYE'
    WHEN v1.votes < v2.votes THEN 'NOE'
    ELSE 'TIE'
    END AS part_alignment
  FROM NonUniVotes n
    LEFT JOIN VotesByPartyAll v1
      ON n.voteId = v1.voteId
    LEFT JOIN VotesByPartyAll v2
      ON v1.voteId = v2.voteId
         AND v1.party = v2.party
  WHERE v1.result = 'AYE'
        AND v2.result = 'NOE';


CREATE OR REPLACE VIEW VotesByPartyGenAlignment
AS
  SELECT v1.voteId,
    v1.AYES AS d_ayes,
    v1.NAES AS d_naes,
    v1.part_alignment AS d_alignment,
    LEAST(v1.AYES, v1.NAES) AS d_min,
    GREATEST(v1.AYES, v1.NAES) AS d_maj,
    v1.AYES + v1.NAES AS d_total,
    v2.AYES AS r_ayes,
    v2.NAES AS r_naes,
    v2.part_alignment AS r_alignment,
    LEAST(v2.AYES, v2.NAES) AS r_min,
    GREATEST(v2.AYES, v2.NAES) AS r_maj,
    v2.AYES + v2.NAES AS r_total,
    IF(v1.part_alignment = v2.part_alignment, 1, 0)
      AS agreement_flag
  FROM VotesByPartyJoined v1
    JOIN VotesByPartyJoined v2
      ON v1.voteId = v2.voteId
  WHERE v1.party = 'Democrat'
        AND v2.party = 'Republican';

-- Throw out the edge cases where there is only one party in
-- the committee
CREATE OR REPLACE VIEW VotesByPartyGenAlignmentFixed
AS
  SELECT *
  FROM VotesByPartyGenAlignment
  WHERE d_total > 0
        AND r_total > 0;


-- Bipartisan scores, higher is better
DROP TABLE IF EXISTS LegTotals;
CREATE TABLE LegTotals
AS
  SELECT v.voteId, d.pid,
    t.party, d.result,
    v.r_alignment, v.d_alignment,
    CASE
    WHEN v.agreement_flag = 1 THEN
      CASE
      WHEN t.party = 'Democrat' AND d.result = v.d_alignment THEN
        v.d_min/v.d_total + v.r_maj/v.r_total
      WHEN t.party = 'Republican' AND d.result = v.r_alignment THEN
        v.r_min/v.r_total + v.d_maj/v.d_total

      WHEN t.party = 'Democrat' AND d.result != v.d_alignment THEN
        -1*(v.d_maj/v.d_total + v.r_min/v.r_total)
      WHEN t.party = 'Republican' AND d.result != v.r_alignment THEN
        -1*(v.r_min/v.r_total + v.d_maj/v.d_total)
      ELSE NULL
      END
    ELSE
      CASE
      WHEN t.party = 'Democrat' AND d.result = v.d_alignment THEN
        -1*(v.d_min/v.d_total + v.r_maj/v.r_total)
      WHEN t.party = 'Republican' AND d.result = v.r_alignment THEN
        -1*(v.r_min/v.r_total + v.d_maj/v.d_total)

      WHEN t.party = 'Democrat' AND d.result != v.d_alignment THEN
        v.d_maj/v.d_total + v.r_min/v.r_total
      WHEN t.party = 'Republican' AND d.result != v.r_alignment THEN
        v.r_min/v.r_total + v.d_maj/v.d_total
      ELSE NULL
      END
    END AS score
  FROM VotesByPartyGenAlignmentFixed v
    JOIN BillVoteDetail d
      ON v.voteId = d.voteId
    JOIN Term t
      ON d.pid = t.pid
  WHERE d.result != 'ABS';
#     AND t.year = 2015;


CREATE OR REPLACE VIEW BipartiScores
AS
  SELECT p.pid,
    p.first,
    p.middle,
    p.last,
    SUM(score) AS score,
    l.party
  FROM LegTotals l
    JOIN Person p
      ON l.pid = p.pid
  GROUP BY p.pid, l.party;

CREATE OR REPLACE VIEW BipartiMinMax
AS
  SELECT MAX(score) AS max,
         MIN(score) AS min
  FROM BipartiScores;

-- This little join on term depends on nobody switching parties
-- Oh uhh also, this only works for CA
CREATE OR REPLACE VIEW BipartiScoresNormalized
AS
  SELECT distinct bs.pid,
                  bs.first,
                  bs.last,
                  bs.party,
                  (bs.score - mm.min) / (mm.max - mm.min) AS score
  FROM BipartiScores bs
    JOIN Term t
      ON bs.pid = t.pid
    JOIN BipartiMinMax mm
  WHERE t.state = 'CA';
#   WHERE t.year = 2015;
















