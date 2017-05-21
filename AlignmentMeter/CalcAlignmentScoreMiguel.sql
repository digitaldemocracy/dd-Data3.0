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
(pid, oid, ba.session_year, abstain_votes, resolution, unanimous)
  SELECT distinct ba.pid, ba.oid, session_year, abstain_votes, resolution, unanimous
  FROM BillAlignmentScoresAndrew ba;

CREATE TEMPORARY TABLE AggScores
AS
  SELECT pid,
    oid,
    session_year,
    abstain_votes,
    resolution,
    unanimous,
  AVG(alignment_percentage) AS score
  FROM BillAlignmentScoresAndrew ba
  GROUP BY ba.pid, ba.oid, session_year, abstain_votes, resolution;

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


drop table CombinedAlignmentScores;
CREATE TABLE CombinedAlignmentScores
AS
  SELECT a.pid,
    a.oid,
    null as house,
    null as party,
    'CA' as 'state',
    a.AndrewScore as score,
    asei.positions_registered,
    asei.votes_in_agreement,
    asei.votes_in_disagreement,
    a.pid as pid_house_party
  FROM AlignmentScores a
    INNER JOIN AlignmentScoresExtraInfo asei
      ON asei.oid = a.oid AND asei.pid = a.pid
  UNION
  SELECT null as pid,
    asa.oid,
    asa.house,
    asa.party,
    asa.state,
    asa.score,
    asa.positions_registered,
    asa.votes_in_agreement,
    asa.votes_in_disagreement,
    CONCAT_WS('',null,house,IF(LENGTH(house) > 0," (", ""),party, IF(LENGTH(house) > 0,")", "")) as pid_house_party
  FROM AlignmentScoresAggregated asa;

alter table CombinedAlignmentScores
    add dr_id int unique AUTO_INCREMENT;

alter table CombinedAlignmentScores
  add INDEX pid_idx (pid),
  add INDEX oid_idx (oid),
  add INDEX state_idx (state),
  add INDEX pid_house_party_idx (pid, house, party);


-- Note this is a one time run to add this data
insert into CombinedAlignmentScores
(pid, oid, house, party, state, score, positions_registered, votes_in_agreement, votes_in_disagreement, pid_house_party)
select null as pid,
  oid,
  null as house,
  null as party,
  'CA' as state,
  avg(score) as score,
  sum(positions_registered) as positions_registered,
  sum(votes_in_agreement) as votes_in_agreement,
  sum(votes_in_disagreement) as votes_in_disagreement,
  null as pid_house_party
from CombinedAlignmentScores
where pid is null
group by oid;


