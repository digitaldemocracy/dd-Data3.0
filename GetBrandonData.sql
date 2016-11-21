# (  a) list of all bills on which there was at least one floor vote
# in the current session.  For each bill we want the following:
#      ID, Name, Principal Author
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
drop table FinalBillVersion;



#     (b) list of all lawmakers who made at least one floor vote
#           Pid,. Name, Party, House, District
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
order by p.pid asc;

#     (c) Organization Bill Support: the last known position of each of
# the 20 organizations we are currently tracking on any of the bills
# from list (a)
#           (note: we only want ONE position of the organization on the
# bill. IT is ok if the data is slightly inconsistent - Brandon just
# needs the dataset to play with it,
#           eventually, we will try it on a more complex dataset).
#
#             BillID, OrgId (MetaID), OrgNAme, PositionOnBill
Raise NotImplementedError()


#     (d) Floor votes on bills from list (a)  (on "do pass" motions)
#              Bill, HearingDate,   House, YesVotes, NoVotes,
# AbstainVotes, Result (Pass/Fail)

CREATE OR REPLACE VIEW DoPassFloorVotes
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
                    c.type,
                    CASE
                        WHEN result = "(PASS)" THEN 1
                        ELSE 0
                    END AS outcome
                FROM BillVoteSummary b
                    JOIN Committee c
                    ON b.cid = c.cid
                    JOIN Motion m
                    on b.mid = m.mid
                WHERE m.text like '%reading%'