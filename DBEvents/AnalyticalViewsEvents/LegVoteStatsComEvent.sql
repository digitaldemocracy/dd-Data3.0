/*
  Calculates Q1 of the Analytical Queries document.
  Question:
  VOTE STATS: For any given legislator, what is the % of bills [votes] on which they vote ‘Aye’, % of [votes] bills on
  which they vote ‘No’, and % of bills [votes] on which they abstain (all locations, all bills, all votes) on which
  they abstain per committee on which the legislator sits.

  **Note**: I interpreted "all votes" to be all votes we considered "passing", not procedural votes.
*/

DROP EVENT IF EXISTS LegVoteStatsCom_event;

DELIMITER |

CREATE EVENT LegVoteStatsCom_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN

    DROP VIEW IF EXISTS TotalCounts;
    DROP VIEW IF EXISTS AyeCounts;
    DROP VIEW IF EXISTS AbsCounts;

    DROP TABLE IF EXISTS TotalCounts;
    DROP TABLE IF EXISTS AyeCounts;
    DROP TABLE IF EXISTS AbsCounts;

    -- All votes  with "do pass" in the motion text
    CREATE OR REPLACE VIEW TotalCounts
    AS
      SELECT
        pid,
        cid,
        count(*) AS count
      FROM AllPassingVotes
      GROUP BY pid, cid;


    CREATE OR REPLACE VIEW AyeCounts
    AS
      SELECT
        pid,
        cid,
        count(*) AS count
      FROM AllPassingVotes
      WHERE leg_vote = 'AYE'
      GROUP BY pid, cid;


    CREATE OR REPLACE VIEW NoeCounts
    AS
      SELECT
        pid,
        cid,
        count(*) AS count
      FROM AllPassingVotes
      WHERE leg_vote = 'NOE'
      GROUP BY pid, cid;


    CREATE OR REPLACE VIEW AbsCounts
    AS
      SELECT
        pid,
        cid,
        count(*) AS count
      FROM AllPassingVotes
      WHERE leg_vote = 'ABS'
      GROUP BY pid, cid;


    DROP TABLE IF EXISTS LegVoteStats;
    CREATE TABLE LegVoteStats
    AS
      SELECT
        t.pid,
        t.cid,
        ifnull(a.count / t.count, 0)  AS aye_pct,
        ifnull(n.count / t.count, 0)  AS noe_pct,
        ifnull(ab.count / t.count, 0) AS abs_pct
      FROM TotalCounts t
        LEFT JOIN AyeCounts a
          ON t.pid = a.pid
             AND t.cid = a.cid
        LEFT JOIN NoeCounts n
          ON t.pid = n.pid
             AND t.cid = n.cid
        LEFT JOIN AbsCounts ab
          ON t.pid = ab.pid
             AND t.cid = ab.cid;


    ALTER TABLE LegVoteStats
      ADD KEY (pid, cid);


    DROP VIEW IF EXISTS TotalCounts;
    DROP VIEW IF EXISTS AyeCounts;
    DROP VIEW IF EXISTS AbsCounts;

  END |

DELIMITER ;
