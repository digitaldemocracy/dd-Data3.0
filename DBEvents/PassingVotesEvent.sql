/*
  Used by AlignmentMeter and BipartisanScores. Keeps track of which votes we consider
  'passing'. Ie interesting enough to count, not just procedural.
*/

DROP EVENT IF EXISTS PassingVotes_event;

DELIMITER |

CREATE EVENT PassingVotes_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN

    DROP VIEW IF EXISTS DoPassVotes;
    DROP VIEW IF EXISTS FloorVotes;
    DROP VIEW IF EXISTS AllPassingVotesTmp;

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
             v.result as vote_outcome,
             bvd.result as leg_vote,
             v.house,
             v.type,
             CONVERT(SUBSTRING(bid, 4, 4), signed int) as session_year,
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
          v.result as vote_outcome,
          bvd.result as leg_vote,
          v.house,
          v.type,
          CONVERT(SUBSTRING(bid, 4, 4), signed int) as session_year,
          naes = 0 or ayes = 0 as unanimous,
          bid like '%ACR%' or bid like '%SCR%' or bid like '%HR%' or bid like '%SR%' or bid like '%AJR%'
          or bid like '%SJR%' as resolution,
        IF(bvd.result = 'ABS', 1, 0) as abstain_vote
        FROM FloorVotes v
          join BillVoteDetail bvd
            on v.voteId = bvd.voteId
        WHERE bid like 'CA%';

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

    DROP VIEW IF EXISTS DoPassVotes;
    DROP VIEW IF EXISTS FloorVotes;
    DROP VIEW IF EXISTS AllPassingVotesTmp;

    END |

DELIMITER ;


