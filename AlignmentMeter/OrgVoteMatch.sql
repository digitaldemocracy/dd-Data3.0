# ----------------------------------------------------------------------
-- CREATES THE OrgVoteMatch TABLE
-- The table will have all legislators votes joined with OrgAlignments

create or replace view AlignmentsWithVotes
  as
  select oa.bid, oa.hid, oa.oid, oa.alignment, oa.oa_id, h.date, s.VoteDate, 
          s.voteId, s.result, bvd.pid, bvd.result as vote
  from OrgAlignments oa
  join Hearing h
  on oa.hid = h.hid
  join BillVoteSummary s
  on oa.bid = s.bid
    and h.date <= s.VoteDate
  join Motion m
  on s.mid = m.mid
    and m.doPass = 1
  join BillVoteDetail bvd 
  on bvd.voteId = s.voteId;


create or replace view MaxDateAlignmentsWithVotes
  as
  select bid, oid, voteId, max(date) as max_date
  from AlignmentsWithVotes
  group by oid, bid, voteId;


create or replace view final
  as
  select awv.voteId, awv.bid, awv.hid, awv.pid, awv.vote, awv.oid, awv.alignment, awv.oa_id, awv.date
  from AlignmentsWithVotes awv
    join MaxDateAlignmentsWithVotes m
    on m.max_date = awv.date
      and m.voteId = awv.voteId
      and m.oid = awv.oid 
      and m.bid = awv.bid;


-- Create the table and drop the views
DROP TABLE IF EXISTS OrgVoteMatch;

CREATE TABLE IF NOT EXISTS OrgVoteMatch
AS (SELECT * FROM final);

DROP VIEW IF EXISTS AlignmentsWithVotes;
DROP VIEW IF EXISTS MaxDateAlignmentsWithVotes;
DROP VIEW IF EXISTS final;


# ------------------------------------------------------------------------
-- FUNCTION TO SCORE WITH ALIGN MATRIX
-- To call it just do VoteAlignment(person_vote, org_position)
  -- Example: VoteAlignment('AYE', 'For') = 1

DROP FUNCTION IF EXISTS VoteAlignment;
DELIMITER //
CREATE FUNCTION VoteAlignment(vote VARCHAR(3), position VARCHAR(25)) 
RETURNS INTEGER
BEGIN 

  -- Declare variables
  DECLARE fors VARCHAR(25); 
  DECLARE for_amended VARCHAR(25);
  DECLARE neutral VARCHAR(25);
  DECLARE indeterminate VARCHAR(25);
  DECLARE na VARCHAR(25);
  DECLARE against_amended VARCHAR(25);
  DECLARE against VARCHAR(25);
  DECLARE aye VARCHAR(3);
  DECLARE noe VARCHAR(3);
  DECLARE abs VARCHAR(3);
  DECLARE val INTEGER DEFAULT 0;

  -- Set variables
  SET fors = "For";
  SET for_amended = "For_if_amend"; 
  SET neutral = "Neutral";
  SET indeterminate = "Indeterminate";
  SET na = "NA";
  SET against_amended = "Against_unless_amend";
  SET against = "Against";
  
  SET aye = "AYE";
  SET noe = "NOE";
  SET abs = "ABS";
  
  -- All the if logic for the matrix
  IF vote = aye AND position = fors THEN SET val = 1;
  ELSEIF vote = abs AND position = fors THEN SET val = -1;
  ELSEIF vote = noe AND position = fors THEN SET val = -1;
  ELSEIF vote = aye AND position = for_amended THEN SET val = 0;
  ELSEIF vote = abs AND position = for_amended THEN SET val = 1;
  ELSEIF vote = noe AND position = for_amended THEN SET val = 0;
  ELSEIF vote = aye AND position = neutral THEN SET val = 0;
  ELSEIF vote = abs AND position = neutral THEN SET val = 1;
  ELSEIF vote = noe AND position = neutral THEN SET val = 0;
  ELSEIF vote = aye AND position = indeterminate THEN SET val = 0;
  ELSEIF vote = abs AND position = indeterminate THEN SET val = 0;
  ELSEIF vote = noe AND position = indeterminate THEN SET val = 0;
  ELSEIF vote = aye AND position = na THEN SET val = 0;
  ELSEIF vote = abs AND position = na THEN SET val = 0;
  ELSEIF vote = noe AND position = na THEN SET val = 0;
  ELSEIF vote = aye AND position = against_amended THEN SET val = -1;
  ELSEIF vote = abs AND position = against_amended THEN SET val = 1;
  ELSEIF vote = noe AND position = against_amended THEN SET val = 1;
  ELSEIF vote = aye AND position = against THEN SET val = -1;
  ELSEIF vote = abs AND position = against THEN SET val = -1;
  ELSEIF vote = noe AND position = against THEN SET val = 1;
  END IF;

  -- Return corresponding matrix value
  RETURN val;

END;
//
DELIMITER ;

# ----------------------------------------------------------------


-- Check all the votes and alignments for that bill and organization
SELECT * FROM OrgVoteMatch WHERE bid like '%AB1056' and oid = 6672;

-- Get all the entries with the org's last alignment (specific to test case)
SELECT x1.*
FROM (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) x1,
      ( select *  from (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) y 
        where date = (SELECT max(date) from (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) xx)) x2
WHERE x1.alignment = x2.alignment
ORDER BY x1.date ASC
;

-- Get the first date when the final alignment was set (specific to test case)
SELECT x1.*
FROM (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) x1,
      ( select *  from (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) y 
        where date = (SELECT max(date) from (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) xx)) x2
WHERE x1.alignment = x2.alignment
ORDER BY x1.date ASC
LIMIT 1
;

-- TODO: Implement getting the votes after the first date when the final alignment was set 
--       for each bill and organization (bid and oid)
-- Currently only works for one bill and oid.. for the current test case.
-- But when I try to make it general I can't seem to because it focuses on just a single date
-- than a group of dates. Sorry if it's not helpful

SELECT o.*
FROM OrgVoteMatch o,
(SELECT x1.*
FROM (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) x1,
      ( select *  from (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) y 
        where date = (SELECT max(date) from (select * from OrgVoteMatch where bid like '%AB1056' and oid = 6672) xx)) x2
WHERE x1.alignment = x2.alignment
ORDER BY x1.date ASC
LIMIT 1) xxx
WHERE o.bid = xxx.bid AND o.oid = xxx.oid AND o.date >= xxx.date
GROUP BY o.pid
;



