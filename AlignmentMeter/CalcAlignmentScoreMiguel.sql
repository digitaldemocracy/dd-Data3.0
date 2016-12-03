INSERT INTO AlignmentScores
(pid, oid)
SELECT distinct ba.pid, ba.oid
FROM BillAlignmentScoresMiguel ba
  LEFT JOIN AlignmentScores a
  ON ba.pid = a.pid
    and ba.oid = a.oid
WHERE a.pid IS NULL
  and a.oid IS NULL;

CREATE TEMPORARY TABLE AggScores
  AS
SELECT pid, oid, AVG(alignment_percentage) AS score
FROM BillAlignmentScoresMiguel ba
GROUP BY ba.pid, ba.oid;

UPDATE AlignmentScores s
    JOIN AggScores a
    ON s.pid = a.pid
      and s.oid = a.oid
    SET s.MiguelScore = a.score;

DROP TABLE AggScores;


# Andrew stuff
INSERT INTO AlignmentScores
(pid, oid)
  SELECT distinct ba.pid, ba.oid
  FROM BillAlignmentScoresAndrew ba
    LEFT JOIN AlignmentScores a
      ON ba.pid = a.pid
         and ba.oid = a.oid
  WHERE a.pid IS NULL
        and a.oid IS NULL;

CREATE TEMPORARY TABLE AggScores
AS
  SELECT pid, oid,
  AVG(alignment_percentage) AS score
  FROM BillAlignmentScoresAndrew ba
  GROUP BY ba.pid, ba.oid;

UPDATE AlignmentScores s
  JOIN AggScores a
    ON s.pid = a.pid
       and s.oid = a.oid
SET s.AndrewScore = a.score;

DROP TABLE AggScores;


delete from AlignmentScoresAggregated;
insert into AlignmentScoresAggregated
(oid, house, party, state, score, votes_in_agreement, votes_in_disagreement,
 positions_registered, affirmations, bills)
select oid,
  house,
  party,
  'CA' as state,
  AVG(s.alignment_percentage) as score,
  sum(s.aligned_votes),
  sum(s.total_votes) - sum(s.aligned_votes),
  sum(positions) as positions,
  sum(affirmations) as affirmations,
  count(distinct bid) as bills
from BillAlignmentScoresAndrew s
  join Term t
  on s.pid = t.pid
where t.year = 2015
group by s.oid, t.party, t.house;


delete from AlignmentScoresExtraInfo;
insert into AlignmentScoresExtraInfo
(oid, pid, votes_in_agreement, votes_in_disagreement,
 positions_registered, affirmations, bills)
select oid,
  pid,
  sum(aligned_votes),
  sum(total_votes) - sum(aligned_votes),
  sum(positions) as positions,
  sum(affirmations) as affirmations,
  count(distinct bid) as bills
from BillAlignmentScoresAndrew
group by oid, pid;


