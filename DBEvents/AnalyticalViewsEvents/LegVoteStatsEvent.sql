/*
  Calculates Q1 of the Analytical Queries document.
  Question:
  VOTE STATS: For any given legislator, what is the % of bills [votes] on which they vote ‘Aye’, % of [votes] bills on
  which they vote ‘No’, and % of bills [votes] on which they abstain (all locations, all bills, all votes)

  **Note**: I interpreted "all votes" to be all votes we considered "passing", not procedural votes.
*/
DROP EVENT IF EXISTS LegVoteStats_event;

DELIMITER |

CREATE EVENT LegVoteStats_event
  ON SCHEDULE
    EVERY 1 DAY STARTS '2017-1-1 07:00:00'
DO
  BEGIN

    DROP VIEW IF EXISTS TotalCounts;
    DROP VIEW IF EXISTS AyeCounts;
    DROP VIEW IF EXISTS AbsCounts;
    DROP VIEW IF EXISTS AllPasssingVotesDup;

    DROP TABLE IF EXISTS TotalCounts;
    DROP TABLE IF EXISTS AyeCounts;
    DROP TABLE IF EXISTS AbsCounts;
    DROP TABLE IF EXISTS AllPasssingVotesDup;

    -- Doubles up every row so thre is also an all category
    DROP TABLE IF EXISTS AllPassingVotesDup;
    CREATE TABLE AllPassingVotesDup
    AS
      SELECT
        pid,
        session_year,
        leg_vote
      FROM AllPassingVotes
      UNION ALL
      SELECT
        pid,
        'All' AS session_year,
        leg_vote
      FROM AllPassingVotes;

    ALTER TABLE AllPassingVotesDup
      ADD KEY (pid, session_year, leg_vote);

    -- All votes  with "do pass" in the motion text
    CREATE OR REPLACE VIEW TotalCounts
    AS
      SELECT
        pid,
        session_year,
        count(*) AS count
      FROM AllPassingVotesDup
      GROUP BY pid, session_year;


    CREATE OR REPLACE VIEW AyeCounts
    AS
      SELECT
        pid,
        session_year,
        count(*) AS count
      FROM AllPassingVotesDup
      WHERE leg_vote = 'AYE'
      GROUP BY pid, session_year;


    CREATE OR REPLACE VIEW NoeCounts
    AS
      SELECT
        pid,
        session_year,
        count(*) AS count
      FROM AllPassingVotesDup
      WHERE leg_vote = 'NOE'
      GROUP BY pid, session_year;


    CREATE OR REPLACE VIEW AbsCounts
    AS
      SELECT
        pid,
        session_year,
        count(*) AS count
      FROM AllPassingVotesDup
      WHERE leg_vote = 'ABS'
      GROUP BY pid, session_year;


    DROP TABLE IF EXISTS LegVoteStats_analyt;
    CREATE TABLE LegVoteStats_analyt
    AS
      SELECT
        t.pid,
        t.session_year,
        ifnull(a.count / t.count, 0)  AS aye_pct,
        ifnull(n.count / t.count, 0)  AS noe_pct,
        ifnull(ab.count / t.count, 0) AS abs_pct
      FROM TotalCounts t
        LEFT JOIN AyeCounts a
          ON t.pid = a.pid AND t.session_year = a.session_year
        LEFT JOIN NoeCounts n
          ON t.pid = n.pid AND t.session_year = n.session_year
        LEFT JOIN AbsCounts ab
          ON t.pid = ab.pid AND t.session_year = ab.session_year;


    ALTER TABLE LegVoteStats_analyt
      ADD KEY (pid, session_year);


    DROP TABLE IF EXISTS AllPassingVotesDup;
    DROP VIEW IF EXISTS TotalCounts;
    DROP VIEW IF EXISTS AyeCounts;
    DROP VIEW IF EXISTS AbsCounts;

  END |

DELIMITER ;
