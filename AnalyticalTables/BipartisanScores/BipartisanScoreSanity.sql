-- Just checking for a reasonable number of these
SELECT COUNT(*)
FROM NonUniVotes;

-- relative to how many votes we have total?
SELECT COUNT(*)
FROM AllPassingVotes;

-- 4x as many as NonUniVotes
SELECT COUNT(*)
FROM DummyVotes;

-- Would be the same as DummyVotes, buut not every
-- party registers a vote in BillVoteDetail
SELECT COUNT(*)
FROM VotesByParty;

SELECT *
FROM VotesByParty
LIMIT 10;

-- Should be equal to the number in dummy votes
SELECT COUNT(*)
FROM VotesByPartyAll;

-- Should be equal to the number in NonUniVotes
SELECT COUNT(DISTINCT voteId)
FROM VotesByPartyAll;

SELECT *
FROM VotesByPartyAll
LIMIT 10;

-- Should be twice as many as in non-uni votes
SELECT COUNT(*)
FROM VotesByPartyJoined;

SELECT *
FROM VotesByPartyJoined
ORDER BY voteId
LIMIT 10;

-- Should be same as number of non-uni votes
SELECT COUNT(*)
FROM VotesByPartyGenAlignment;

SELECT *
FROM VotesByPartyGenAlignment
LIMIT 10;

-- It's important that this comes up empty
SELECT *
FROM VotesByPartyGenAlignment
WHERE d_total = 0
    OR r_total = 0;


-- After you threw out edge cases
SELECT COUNT(*)
FROM VotesByPartyGenAlignmentFixed;

-- Now it comes up empty
SELECT *
FROM VotesByPartyGenAlignmentFixed
WHERE d_total = 0
    OR r_total = 0;

-- This should also come up empty
SELECT *
FROM VotesByPartyGenAlignmentFixed
WHERE CONCAT(voteId, d_ayes, d_naes, d_alignment,
d_min, d_maj, d_total, r_ayes, r_naes, r_alignment,
r_min, r_maj, r_total, agreement_flag) IS NULL;


-- These two should really be equal
SELECT COUNT(DISTINCT voteId, pid)
FROM LegTotals;

SELECT COUNT(*)
FROM LegTotals;

-- Same as number of NonUniVotes (so close...)
SELECT COUNT(DISTINCT voteId)
FROM LegTotals;

SELECT *
FROM LegTotals
LIMIT 10;

-- Should come up empty
SELECT *
FROM LegTotals
WHERE score IS NULL;


SELECT *
FROM BipartiScores
ORDER BY score DESC
LIMIT 20;

SELECT *
FROM BipartiScores
ORDER BY score ASC
LIMIT 20;
