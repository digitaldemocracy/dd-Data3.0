-- # (  a) list of all bills on which there was at least one floor vote
-- # in the current session.  For each bill we want the following:
-- #      ID, Name, Principal Author
create view FinalDate
  as
select bid, max(date) as max_date
from BillVersion
group by bid;

create TEMPORARY TABLE FinalBillVersion
  as
select bv.bid, bv.vid, bv.subject
from BillVersion bv
  join FinalDate fd
  on bv.bid = fd.bid
    and bv.date = fd.max_date;

alter table FinalBillVersion
    add index bid_idx (bid);

select distinct bvs.bid, fbv.subject as bill_subject, a.pid, p.first, p.middle, p.last
from BillVoteSummary bvs
  join Committee c
  on bvs.cid = c.cid
  join FinalBillVersion fbv
  on bvs.bid = fbv.bid
  join authors a
  on bvs.bid = a.bid
    and fbv.vid = a.vid
  join Person p
  on a.pid = p.pid
where c.name like '%floor%'
  and c.state = 'CA'
  and a.contribution = 'Lead Author';

drop view FinalDate;



-- #     (b) list of all lawmakers who made at least one floor vote
-- #           Pid,. Name, Party, House, District
select distinct p.pid, p.first, p.middle, p.last, t.party, t.house, t.district
from BillVoteSummary bvs
  join BillVoteDetail bvd
  on bvs.voteId = bvd.voteId
  join Committee c
  on bvs.cid = c.cid
  join Person p
  on bvd.pid = p.pid
  join Term t
  on t.pid = p.pid
where c.name like '%floor%'
  and t.year = 2015
  and t.state = 'CA'
order by p.pid asc;

-- #     (c) Organization Bill Support: the last known position of each of
-- # the 20 organizations we are currently tracking on any of the bills
-- # from list (a)
-- #           (note: we only want ONE position of the organization on the
-- # bill. IT is ok if the data is slightly inconsistent - Brandon just
-- # needs the dataset to play with it,
-- #           eventually, we will try it on a more complex dataset).
-- #
-- #             BillID, OrgId (MetaID), OrgNAme, PositionOnBill
create TEMPORARY table LastAlignmentDates
as
select b.bid, o.oid, o.name, max(h.date) as last_date
from OrgConcept o
  join OrgConceptAffiliation oca
  on o.oid = oca.new_oid
  join OrgAlignments oa
  on oca.old_oid = oa.oid
  join FinalBillVersion b
  on oa.bid = b.bid
  join Hearing h
  on oa.hid = h.hid
group by b.bid, o.oid, o.name;

alter table LastAlignmentDates
add index bid_idx (bid),
add index oid_idx (oid),
add index date_idx (last_date);

select distinct lad.bid, lad.oid, lad.name, oa.alignment as position_on_bill
from LastAlignmentDates lad
  join OrgConceptAffiliation oca
  on lad.oid = oca.new_oid
  join OrgAlignments oa
  on lad.bid = oa.bid
    and oca.old_oid = oa.oid
  join Hearing h
  on oa.hid = h.hid
    and lad.last_date = h.date;

-- just check this. Should be the same
select count(*)
from LastAlignmentDates lad
  join OrgConceptAffiliation oca
  on lad.oid = oca.new_oid
  join OrgAlignments oa
  on lad.bid = oa.bid
    and oca.old_oid = oa.oid
  join Hearing h
  on oa.hid = h.hid
    and lad.last_date = h.date;

select count(distinct lad.bid, lad.oid, lad.name, oa.alignment)
from LastAlignmentDates lad
  join OrgConceptAffiliation oca
  on lad.oid = oca.new_oid
  join OrgAlignments oa
  on lad.bid = oa.bid
    and oca.old_oid = oa.oid
  join Hearing h
  on oa.hid = h.hid
    and lad.last_date = h.date;


drop table FinalBillVersion;
drop table LastAlignmentDates;



-- #     (d) Floor votes on bills from list (a)  (on "do pass" motions)
-- #              Bill, HearingDate,   House, YesVotes, NoVotes,
-- # AbstainVotes, Result (Pass/Fail)

SELECT b.bid,
    b.VoteDate,
    c.house,
    b.ayes,
    b.naes,
    b.abstain,
    b.result
FROM BillVoteSummary b
    JOIN Committee c
    ON b.cid = c.cid
    JOIN Motion m
    on b.mid = m.mid
WHERE m.text like '%reading%';



-- (e) individual lawmaker votes on the floor votes from list (d):
-- Bill, HearingDate, House, LawmakerId, LawmakerName, Vote
SELECT b.bid,
    b.VoteDate,
    c.house,
    p.pid as LawmakerId,
    p.first,
    p.last,
    bvd.result,
    t.party
FROM BillVoteSummary b
    JOIN Committee c
    ON b.cid = c.cid
    JOIN Motion m
    on b.mid = m.mid
    join BillVoteDetail bvd
    on b.voteId = bvd.voteId
    join Person p
    on p.pid = bvd.pid
    join Term t
    on t.pid = p.pid
WHERE m.text like '%reading%'
  and t.year = 2015;
