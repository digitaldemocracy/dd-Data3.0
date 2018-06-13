# Senior Project Queries

select pid, bid
from (select * from Term where state = 'CA'
      and year = 2015) as Legs2015,
     (select * from Bill where state = 'CA'
      and sessionYear = 2015) as Bills2015;

select * from BillSponsors;

# flag for author of bill
select pid, bid, if(exists(select * from BillSponsors bs
              where bs.pid = Legs2015.pid
              and bs.bid = Bills2015.bid), 1, 0) as author_flag,
  if(exists(select * from BillSponsors bs
              where bs.pid = Legs2015.pid
              and bs.bid = Bills2015.bid
              and bs.contribution = 'Coauthor'), 1, 0) as co_author_flag,
  if(exists(select * from BillSponsors bs
              where bs.pid = Legs2015.pid
              and bs.bid = Bills2015.bid
              and bs.contribution = 'Lead Author'), 1, 0) as lead_author_flag
from (select * from Term where state = 'CA'
      and year = 2015) as Legs2015,
     (select * from Bill where state = 'CA'
      and sessionYear = 2015) as Bills2015;

SELECT distinct contribution from BillSponsors;

# how many votes did a legislator take on a bill
select pid, bid, (select count(*)
                    from BillVoteDetail bvd join BillVoteSummary bvs
                    on bvd.voteId = bvs.voteId
                      where bvd.pid = Legs2015.pid
                      and bvs.bid = Bills2015.bid
                    group by bvd.pid, bvs.bid) as voteCount
from (select * from Term where state = 'CA'
      and year = 2015) as Legs2015,
     (select * from Bill where state = 'CA'
      and sessionYear = 2015) as Bills2015;

# how many bill discussions did a legislator participate in on a bill?
# bdCount_all: all bill discussions
# bdCount_comm: bill discussions where leg was on committee hosting discussion
# bdCount_not_comm: BDs where leg was not on committee hosting discussion
select pid, bid, if(exists(select count(*) as BDCounts
                    from Utterance u join BillDiscussion bd on u.did = bd.did
                    where bd.bid = Bills2015.bid
                    and u.pid = Legs2015.pid
                    GROUP BY bd.bid, u.pid), (select count(*) as BDCounts
                    from Utterance u join BillDiscussion bd on u.did = bd.did
                    where bd.bid = Bills2015.bid
                    and u.pid = Legs2015.pid
                    GROUP BY bd.bid, u.pid), 0) as bdCount_all,
  if(exists(select count(*) as BDCounts
                    from Utterance u
                      join BillDiscussion bd on u.did = bd.did
                      join Hearing h on bd.hid = h.hid
                      join CommitteeHearings ch on h.hid = ch.hid
                    where bd.bid = Bills2015.bid
                    and u.pid = Legs2015.pid
                    and ch.cid in (select cid from servesOn where pid = Legs2015.pid)
                    GROUP BY bd.bid, u.pid),
     (select count(*) as BDCounts
                    from Utterance u
                      join BillDiscussion bd on u.did = bd.did
                      join Hearing h on bd.hid = h.hid
                      join CommitteeHearings ch on h.hid = ch.hid
                    where bd.bid = Bills2015.bid
                    and u.pid = Legs2015.pid
                    and ch.cid in (select cid from servesOn where pid = Legs2015.pid)
                    GROUP BY bd.bid, u.pid), 0) as bdCount_comm,
  if(exists(select count(*) as BDCounts
                    from Utterance u
                      join BillDiscussion bd on u.did = bd.did
                      join Hearing h on bd.hid = h.hid
                      join CommitteeHearings ch on h.hid = ch.hid
                    where bd.bid = Bills2015.bid
                    and u.pid = Legs2015.pid
                    and ch.cid not in (select cid from servesOn where pid = Legs2015.pid)
                    GROUP BY bd.bid, u.pid),
     (select count(*) as BDCounts
                    from Utterance u
                      join BillDiscussion bd on u.did = bd.did
                      join Hearing h on bd.hid = h.hid
                      join CommitteeHearings ch on h.hid = ch.hid
                    where bd.bid = Bills2015.bid
                    and u.pid = Legs2015.pid
                    and ch.cid not in (select cid from servesOn where pid = Legs2015.pid)
                    GROUP BY bd.bid, u.pid), 0) as bdCount_not_comm
from (select * from Term where state = 'CA'
      and year = 2015) as Legs2015,
     (select * from Bill where state = 'CA'
      and sessionYear = 2015) as Bills2015;


# Actual BD count queries
# All BDs
select pid, bid, count(*) as bdCount_all
from Utterance u
  join BillDiscussion bd on u.did = bd.did
  join Hearing h on bd.hid = h.hid
  join CommitteeHearings ch on h.hid = ch.hid
where pid in (select pid from Term where state = 'CA'
      and year = 2015)
and bid in (select bid from Bill where state = 'CA'
      and sessionYear = 2015)
GROUP BY u.pid, bd.bid;

# Where legislator is committee member
select pid, bid, count(*) as bdCount_comm
from Utterance u
  join BillDiscussion bd on u.did = bd.did
  join Hearing h on bd.hid = h.hid
  join CommitteeHearings ch on h.hid = ch.hid
where pid in (select pid from Term where state = 'CA'
      and year = 2015)
and bid in (select bid from Bill where state = 'CA'
      and sessionYear = 2015)
and ch.cid in (select cid from servesOn where pid in (select pid from Term where state = 'CA'
      and year = 2015))
GROUP BY u.pid, bd.bid;

# Where legislator is not a committee member
select pid, bid, count(*) as bdCount_no_comm
from Utterance u
  join BillDiscussion bd on u.did = bd.did
  join Hearing h on bd.hid = h.hid
  join CommitteeHearings ch on h.hid = ch.hid
where pid in (select pid from Term where state = 'CA'
      and year = 2015)
and bid in (select bid from Bill where state = 'CA'
      and sessionYear = 2015)
and ch.cid not in (select cid from servesOn where pid in (select pid from Term where state = 'CA'
      and year = 2015))
GROUP BY u.pid, bd.bid;

# Number of words said on a bill
# wordCount_all: Total word count from all bill discussions
# wordCount_comm: Word count from bill discussions where leg was on committee hosting discussion
# wordCount_not_comm: Word count from BDs where leg not on committee hosting discussion
#
# wordCount_comm and wordCount_not_comm CURRENTLY NOT WORKING
select pid, bid,
  if(exists(select 1 from currentUtterance u
                           join BillDiscussion bd on u.did = bd.did
                           where bd.bid = Bills2015.bid
                           and u.pid = Legs2015.pid),
                   (select sum(wordCount) from
                   (select pid, bid, length(text) - length(replace(text, ' ', '')) as wordCount
                    from currentUtterance u join BillDiscussion bd on u.did = bd.did) as wordCounts
                    where pid = Legs2015.pid
                    and bid = Bills2015.bid
                    group by pid, bid), 0) as wordCount_all,
  if(exists(select 1
            from currentUtterance u
              join BillDiscussion bd on u.did = bd.did
              join Hearing h on bd.hid = h.hid
              join CommitteeHearings ch on h.hid = ch.hid
            where bd.bid = Bills2015.bid
              and u.pid = Legs2015.pid
              and ch.hid in (select cid from servesOn where pid=u.pid)),
    (select sum(wordCount) from
      (select pid, bid, length(text) - length(replace(text, ' ', '')) as wordCount
       from currentUtterance u
         join BillDiscussion bd on u.did = bd.did
         join Hearing h on bd.hid = h.hid
         join CommitteeHearings ch on h.hid = ch.hid
       where ch.cid not in (SELECT cid from servesOn where pid = u.pid)
        ) as wordCounts
       where pid = Legs2015.pid
       and bid = Bills2015.bid
       group by pid, bid), 0) as wordCount_comm,
  if(exists(select 1
            from currentUtterance u
              join BillDiscussion bd on u.did = bd.did
              join Hearing h on bd.hid = h.hid
              join CommitteeHearings ch on h.hid = ch.hid
            where bd.bid = Bills2015.bid
              and u.pid = Legs2015.pid
              and ch.hid not in (select cid from servesOn where pid=u.pid)),
    (select sum(wordCount) from
      (select pid, bid, length(text) - length(replace(text, ' ', '')) as wordCount
       from currentUtterance u
         join BillDiscussion bd on u.did = bd.did
         join Hearing h on bd.hid = h.hid
         join CommitteeHearings ch on h.hid = ch.hid
       where ch.cid not in (SELECT cid from servesOn where pid = u.pid)
        ) as wordCounts
       where pid = Legs2015.pid
       and bid = Bills2015.bid
       group by pid, bid), 0) as wordCount_not_comm
from (select * from Term where state = 'CA'
      and year = 2015) as Legs2015,
     (select * from Bill where state = 'CA'
      and sessionYear = 2015) as Bills2015;

# Actual word count query
select pid, bid, sum(wordCount) as wordCount from
(select pid, bid, length(text) - length(replace(text, ' ', '')) as wordCount
from currentUtterance u join BillDiscussion bd on u.did = bd.did
where pid in (select pid from Term where state = 'CA'
      and year = 2015)
and bid in (select bid from Bill where state = 'CA'
      and sessionYear = 2015)) as wordCounts
group by pid, bid;

# Total duration of utterances on a certain bill
# utteranceDuration_all: Total duration of all utterances on bill
# utteranceDuration_comm: Duration of utterances on bill discussions where leg part of hosting committee
# utteranceDuration_not_comm: Duration of utterances on BDs where leg not on hosting committee
select pid, bid,
  if(exists(select * from currentUtterance u
            join BillDiscussion bd on u.did = bd.did
            where u.pid = Legs2015.pid
            and bd.bid = Bills2015.bid),
     (select sum(u.endTime - u.time)
      from currentUtterance u
      join BillDiscussion bd on u.did = bd.did
      where u.pid = Legs2015.pid
      and bd.bid = Bills2015.bid
      group by u.pid, bd.bid), 0) as utteranceDuration_all,
  if(exists(select * from currentUtterance u
            join BillDiscussion bd on u.did = bd.did
            join Hearing h on bd.hid = h.hid
            join CommitteeHearings ch on h.hid = ch.hid
            where u.pid = Legs2015.pid
            and bd.bid = Bills2015.bid
            and ch.cid in (select cid from servesOn where pid = u.pid)),
     (select sum(u.endTime - u.time)
      from currentUtterance u
      join BillDiscussion bd on u.did = bd.did
      join Hearing h on bd.hid = h.hid
      join CommitteeHearings ch on h.hid = ch.hid
      where u.pid = Legs2015.pid
      and bd.bid = Bills2015.bid
      and ch.cid in (select cid from servesOn where pid = u.pid)
      group by u.pid, bd.bid), 0) as utteranceDuration_comm,
  if(exists(select * from currentUtterance u
            join BillDiscussion bd on u.did = bd.did
            join Hearing h on bd.hid = h.hid
            join CommitteeHearings ch on h.hid = ch.hid
            where u.pid = Legs2015.pid
            and bd.bid = Bills2015.bid
            and ch.cid not in (select cid from servesOn where pid = u.pid)),
     (select sum(u.endTime - u.time)
      from currentUtterance u
      join BillDiscussion bd on u.did = bd.did
      join Hearing h on bd.hid = h.hid
      join CommitteeHearings ch on h.hid = ch.hid
      where u.pid = Legs2015.pid
      and bd.bid = Bills2015.bid
      and ch.cid not in (select cid from servesOn where pid = u.pid)
      group by u.pid, bd.bid), 0) as utteranceDuration_not_comm
from (select pid from Term where state = 'CA'
      and year = 2015) as Legs2015,
     (select bid from Bill where state = 'CA'
      and sessionYear = 2015) as Bills2015;

select Legs2015.pid, Bills2015.bid,
  CASE WHEN utteranceTimes.times IS NULL THEN 0 ELSE utteranceTimes.times END
    as utteranceDuration_all
from (select pid from Term where state = 'CA'
      and year = 2015) as Legs2015,
     (select bid from Bill where state = 'CA'
      and sessionYear = 2015) as Bills2015
LEFT JOIN (select pid, bid, sum(u.endTime-u.time) as times
from currentUtterance u
join BillDiscussion bd on u.did = bd.did
group by pid, bid) as utteranceTimes on utteranceTimes.pid = pid
                                    and utteranceTimes.bid = Bills2015.bid;


# Utterance duration all
select pid, bid, sum(u.endTime - u.time) as utteranceDuration_all
      from currentUtterance u
      join BillDiscussion bd on u.did = bd.did
      where u.pid in (select pid from Term where state = 'CA'
      and year = 2015)
      and bd.bid in (select bid from Bill where state = 'CA'
      and sessionYear = 2015)
      group by u.pid, bd.bid;

# Utterance duration comm
select pid, bid, sum(u.endTime - u.time) as utteranceDuration_comm
      from currentUtterance u
      join BillDiscussion bd on u.did = bd.did
      join Hearing h on bd.hid = h.hid
      join CommitteeHearings ch on h.hid = ch.hid
      where u.pid in (select pid from Term where state = 'CA'
      and year = 2015)
      and bd.bid in (select bid from Bill where state = 'CA'
      and sessionYear = 2015)
      and ch.cid in (select cid from servesOn where pid = u.pid)
      group by u.pid, bd.bid;

# Utterance duration not comm
select pid, bid, sum(u.endTime - u.time) as utteranceDuration_not_comm
      from currentUtterance u
      join BillDiscussion bd on u.did = bd.did
      join Hearing h on bd.hid = h.hid
      join CommitteeHearings ch on h.hid = ch.hid
      where u.pid in (select pid from Term where state = 'CA'
      and year = 2015)
      and bd.bid in (select bid from Bill where state = 'CA'
      and sessionYear = 2015)
      and ch.cid not in (select cid from servesOn where pid = u.pid)
      group by u.pid, bd.bid;

# Vote Count Aye
select pid, bid, count(bvd.voteId) as ayeVotes
from BillVoteDetail bvd join BillVoteSummary bvs
on bvd.voteId = bvs.voteId
where bvd.result = 'AYE'
and pid in (select pid from Term where state = 'CA'
            and year = 2015)
and bid in (select bid from Bill where state = 'CA'
            and sessionYear = 2015)
group by pid, bid;

# Vote Count Nae
select pid, bid, count(bvd.voteId) as naeVotes
from BillVoteDetail bvd join BillVoteSummary bvs
on bvd.voteId = bvs.voteId
where bvd.result = 'NOE'
and pid in (select pid from Term where state = 'CA'
            and year = 2015)
and bid in (select bid from Bill where state = 'CA'
            and sessionYear = 2015)
group by pid, bid;

# Vote Count Abstain
select pid, bid, count(bvd.voteId) as abstainVotes
from BillVoteDetail bvd join BillVoteSummary bvs
on bvd.voteId = bvs.voteId
where bvd.result = 'ABS'
and pid in (select pid from Term where state = 'CA'
            and year = 2015)
and bid in (select bid from Bill where state = 'CA'
            and sessionYear = 2015)
group by pid, bid;

# Unanimous Flag
select pid, bid, if(exists(select voteId from BillVoteSummary b2
                where bid = Bills2015.bid
                and naes != 0), 0, 1) as is_unanimous
from
  (select * from Bill where state = 'CA' and sessionYear = 2015) Bills2015,
  (select * from Term where state = 'CA' and year = 2015) Legs2015;


# Get a bill's final subject
select b.bid, bv.subject from Bill b
left join BillVersion bv on b.bid = bv.bid
join (select bv.bid, max(bv.date) as maxDate
from BillVersion bv
group by bv.bid) maxDate on bv.date = maxDate.maxDate and bv.bid=maxDate.bid
where b.sessionYear = 2015
and b.state = 'CA';
