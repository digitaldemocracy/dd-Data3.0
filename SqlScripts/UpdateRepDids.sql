
-- This is the simple case where every bad rep row, has a single dummy bill discussion that corresponds to the hearing
-- perfectly. first two counts need to be the same before running update
select count(distinct lr.pid, lr.oid, bd.did)
from LobbyistRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where lr.did = 0
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

select count(distinct lr.pid, lr.oid, lr.hid)
from LobbyistRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where lr.did = 0
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

update LobbyistRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
set lr.did = bd.did
where lr.did = 0
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');


-- Clear cut case of one bill discussion
select *
from LobbyistRepresentation lr
  join (select hid, did
        from BillDiscussion bd
        group by hid
        having count(distinct did) = 1) t
    on lr.hid = t.hid
  join BillDiscussion bd
    on lr.did = bd.did
where lr.hid != bd.hid;


update LobbyistRepresentation lr
  join (select hid, did
        from BillDiscussion bd
        group by hid
        having count(distinct did) = 1) t
    on lr.hid = t.hid
  join BillDiscussion bd
    on lr.did = bd.did
set lr.did = t.did
where lr.hid != bd.hid;

select *
from GeneralPublic lr
  join (select hid, did
        from BillDiscussion bd
        group by hid
        having count(distinct did) = 1) t
    on lr.hid = t.hid
  join BillDiscussion bd
    on lr.did = bd.did
where lr.hid != bd.hid;


update GeneralPublic lr
  join (select hid, did
        from BillDiscussion bd
        group by hid
        having count(distinct did) = 1) t
    on lr.hid = t.hid
  join BillDiscussion bd
    on lr.did = bd.did
set lr.did = t.did
where lr.hid != bd.hid;

-- This case is the same as above, but for general public
select count(distinct lr.pid, lr.oid, bd.did)
from GeneralPublic lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

select count(distinct lr.pid, lr.oid, lr.hid)
from GeneralPublic lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

update GeneralPublic lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
set lr.did = bd.did
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');



-- These appear to have no possible bill discussions associated with the hearing
select *
from LobbyistRepresentation lr
  left join BillDiscussion bd
  on lr.hid = bd.hid
where lr.did = 0
  and bd.did is null;

select distinct lr.hid, lr.state
from LobbyistRepresentation lr
  left join BillDiscussion bd
    on lr.hid = bd.hid
where lr.did = 0
      and bd.did is null;

create view video_limits
  as
select h.hid, min(t.vid) as start_video, max(t.vid) as end_video
from Hearing h
  join TT_Task t
  on h.hid = t.hid
group by h.hid;

insert into BillDiscussion
(bid, hid, startVideo, endVideo, numVideos)
select distinct if(lr.state = 'CA', 'CA_NO BILL DISCUSSED', 'NY_NO BILL DISCUSSED'),
  lr.hid,
  vl.start_video,
  vl.end_video,
  vl.end_video - vl.start_video + 1 as num_vidoes
from LobbyistRepresentation lr
  left join BillDiscussion bd
    on lr.hid = bd.hid
  join video_limits vl
    on lr.hid = vl.hid
where lr.did = 0
      and bd.did is null;

-- Just get your last five did's. Don't try and get cute
select *
from BillDiscussion
order by lastTouched desc;

18279	91
18281	33
18282	79
18278	96
18280	44

update LobbyistRepresentation
set did = 18279
where hid = 91;

update LobbyistRepresentation
set did = 18281
where hid = 33;

update LobbyistRepresentation
set did = 18282
where hid = 79;

update LobbyistRepresentation
set did = 18278
where hid = 96;

update LobbyistRepresentation
set did = 18280
where hid = 44;


-- Samesies for GP
select *
from GeneralPublic lr
  left join BillDiscussion bd
  on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
  and bd.did is null;

select distinct lr.hid, lr.state
from GeneralPublic lr
  left join BillDiscussion bd
    on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
      and bd.did is null;

insert into BillDiscussion
(bid, hid, startVideo, endVideo, numVideos)
  select distinct if(lr.state = 'CA', 'CA_NO BILL DISCUSSED', 'NY_NO BILL DISCUSSED'),
    lr.hid,
    vl.start_video,
    vl.end_video,
    vl.end_video - vl.start_video + 1 as num_vidoes
  from GeneralPublic lr
    left join BillDiscussion bd
      on lr.hid = bd.hid
    join video_limits vl
      on lr.hid = vl.hid
  where (lr.did = 0 or lr.did is null)
        and bd.did is null;

-- Just get your last one did's. Don't try and get cute
create TEMPORARY TABLE last_bds
  as
select hid, did
from BillDiscussion
order by lastTouched desc
limit 22;

18285	63

update GeneralPublic gp
  join last_bds l
  on gp.hid = l.hid
set gp.did = l.did;

drop view video_limits;

-- We'll try and get some of the remaining ones based on uttereance dids
select count(distinct lr.pid, u.did)
from LobbyistRepresentation lr
  join Video v
  on lr.hid = v.hid
  join currentUtterance u
  on v.vid = u.vid
    and lr.pid = u.pid
where lr.did = 0;

-- smaller number but whatever, we're still gaining data
select count(distinct lr.pid)
from LobbyistRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0;

update LobbyistRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
set lr.did = u.did
where lr.did = 0 or lr.did is null;

-- Now for GP
select count(distinct lr.pid, u.did)
from GeneralPublic lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

-- smaller number but whatever, we're still gaining data
select count(distinct lr.pid)
from GeneralPublic lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

update GeneralPublic lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
    join BillDiscussion bd
    on u.did = bd.did
set lr.did = u.did
where lr.did = 0 or lr.did is null;



-- Okay what's left
select count(*)
from LobbyistRepresentation lr
where did = 0 or did is null;

select count(*)
from LobbyistRepresentation lr
  join Video v
  on lr.hid = v.hid
where did = 0 or did is null;

select count(distinct u.did)
from LobbyistRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
      and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

delete lr from LobbyistRepresentation lr
  join Video v
    on lr.hid = v.hid
  left join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where (lr.did = 0 or lr.did is null)
    and u.pid is null;


-- For GP
select count(*)
from GeneralPublic lr
where did = 0 or did is null;

select count(*)
from GeneralPublic lr
  join Video v
    on lr.hid = v.hid
where did = 0 or did is null;

select count(distinct u.did)
from GeneralPublic lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

select distinct u.did
from GeneralPublic lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

delete lr from GeneralPublic lr
  join Video v
    on lr.hid = v.hid
  left join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where (lr.did = 0 or lr.did is null)
      and u.pid is null;


-- What's left?
select count(*)
from GeneralPublic
where did = 0 or did is null;

select count(*)
from LobbyistRepresentation
where did = 0 or did is null;


-- Same hassle for LegislativeStaffRepresentation
-- This case is the same as above, but for general public
select count(distinct lr.pid, bd.did)
from LegislativeStaffRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

select count(distinct lr.pid, lr.hid)
from LegislativeStaffRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

update LegislativeStaffRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
set lr.did = bd.did
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

-- Samesies for GP
select *
from LegislativeStaffRepresentation lr
  left join BillDiscussion bd
  on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
  and bd.did is null;

select distinct lr.hid, lr.state
from LegislativeStaffRepresentation lr
  left join BillDiscussion bd
    on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
      and bd.did is null;

insert into BillDiscussion
(bid, hid, startVideo, endVideo, numVideos)
  select distinct if(lr.state = 'CA', 'CA_NO BILL DISCUSSED', 'NY_NO BILL DISCUSSED'),
    lr.hid,
    vl.start_video,
    vl.end_video,
    vl.end_video - vl.start_video + 1 as num_vidoes
  from LegislativeStaffRepresentation lr
    left join BillDiscussion bd
      on lr.hid = bd.hid
    join video_limits vl
      on lr.hid = vl.hid
  where (lr.did = 0 or lr.did is null)
        and bd.did is null;

-- Just get your last one did's. Don't try and get cute
create TEMPORARY TABLE last_bds
  as
select hid, did
from BillDiscussion
order by lastTouched desc
limit 22;

18285	63

update LegislativeStaffRepresentation gp
  join last_bds l
  on gp.hid = l.hid
set gp.did = l.did;

drop view video_limits;

-- Now for GP
select count(distinct lr.pid, u.did)
from LegislativeStaffRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

-- smaller number but whatever, we're still gaining data
select count(distinct lr.pid, v.hid)
from LegislativeStaffRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

update LegislativeStaffRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
    join BillDiscussion bd
    on u.did = bd.did
set lr.did = u.did
where lr.did = 0 or lr.did is null;

-- For GP
select count(*)
from LegislativeStaffRepresentation lr
where did = 0 or did is null;

select count(*)
from LegislativeStaffRepresentation lr
  join Video v
    on lr.hid = v.hid
where did = 0 or did is null;

select count(distinct u.did)
from LegislativeStaffRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

select distinct u.did
from LegislativeStaffRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

delete lr from LegislativeStaffRepresentation lr
  join Video v
    on lr.hid = v.hid
  left join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where (lr.did = 0 or lr.did is null)
      and u.pid is null;

-- What's left?
select count(*)
from LegislativeStaffRepresentation
where did = 0 or did is null;

-- Same hassle for StateAgencyRepRepresentation
-- This case is the same as above, but for general public
select count(distinct lr.pid, bd.did)
from StateAgencyRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

select count(distinct lr.pid, lr.hid)
from StateAgencyRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

update StateAgencyRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
set lr.did = bd.did
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

-- Samesies for GP
select *
from StateAgencyRepRepresentation lr
  left join BillDiscussion bd
  on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
  and bd.did is null;

select distinct lr.hid, lr.state
from StateAgencyRepRepresentation lr
  left join BillDiscussion bd
    on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
      and bd.did is null;

insert into BillDiscussion
(bid, hid, startVideo, endVideo, numVideos)
  select distinct if(lr.state = 'CA', 'CA_NO BILL DISCUSSED', 'NY_NO BILL DISCUSSED'),
    lr.hid,
    vl.start_video,
    vl.end_video,
    vl.end_video - vl.start_video + 1 as num_vidoes
  from StateAgencyRepRepresentation lr
    left join BillDiscussion bd
      on lr.hid = bd.hid
    join video_limits vl
      on lr.hid = vl.hid
  where (lr.did = 0 or lr.did is null)
        and bd.did is null;

-- Just get your last one did's. Don't try and get cute
create TEMPORARY TABLE last_bds
  as
select hid, did
from BillDiscussion
order by lastTouched desc
limit 22;

18285	63

update StateAgencyRepRepresentation gp
  join last_bds l
  on gp.hid = l.hid
set gp.did = l.did;

drop view video_limits;

-- Now for GP
select count(distinct lr.pid, u.did)
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

-- smaller number but whatever, we're still gaining data
select count(distinct lr.pid)
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

update StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
    join BillDiscussion bd
    on u.did = bd.did
set lr.did = u.did
where lr.did = 0 or lr.did is null;

-- For GP
select count(*)
from StateAgencyRepRepresentation lr
where did = 0 or did is null;

select count(*)
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
where did = 0 or did is null;

select count(distinct u.did)
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

select distinct u.did
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

delete lr from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  left join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where (lr.did = 0 or lr.did is null)
      and u.pid is null;

-- What's left?
select count(*)
from StateAgencyRepRepresentation
where did = 0 or did is null;


-- Same hassle for StateAgencyRepRepresentation
-- This case is the same as above, but for general public
select count(distinct lr.pid, bd.did)
from StateAgencyRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

select count(distinct lr.pid, lr.hid)
from StateAgencyRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

update StateAgencyRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
set lr.did = bd.did
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

-- Samesies for GP
select *
from StateAgencyRepRepresentation lr
  left join BillDiscussion bd
  on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
  and bd.did is null;

select distinct lr.hid, lr.state
from StateAgencyRepRepresentation lr
  left join BillDiscussion bd
    on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
      and bd.did is null;

create view video_limits
  as
select h.hid, min(t.vid) as start_video, max(t.vid) as end_video
from Hearing h
  join TT_Task t
  on h.hid = t.hid
group by h.hid;

insert into BillDiscussion
(bid, hid, startVideo, endVideo, numVideos)
  select distinct if(lr.state = 'CA', 'CA_NO BILL DISCUSSED', 'NY_NO BILL DISCUSSED'),
    lr.hid,
    vl.start_video,
    vl.end_video,
    vl.end_video - vl.start_video + 1 as num_vidoes
  from StateAgencyRepRepresentation lr
    left join BillDiscussion bd
      on lr.hid = bd.hid
    join video_limits vl
      on lr.hid = vl.hid
  where (lr.did = 0 or lr.did is null)
        and bd.did is null;

drop view video_limits;

-- Just get your last one did's. Don't try and get cute
create TEMPORARY TABLE last_bds
  as
select hid, did
from BillDiscussion
order by lastTouched desc
limit 2;

select *
from last_bds;

update StateAgencyRepRepresentation gp
  join last_bds l
  on gp.hid = l.hid
set gp.did = l.did;


drop table last_bds;

-- Now for GP
select count(distinct lr.pid, u.did)
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

-- smaller number but whatever, we're still gaining data
select count(distinct lr.pid)
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

update StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
    join BillDiscussion bd
    on u.did = bd.did
set lr.did = u.did
where lr.did = 0 or lr.did is null;


-- For GP
select count(*)
from StateAgencyRepRepresentation lr
where did = 0 or did is null;

select count(*)
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
where did = 0 or did is null;

select count(distinct u.did)
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

select distinct u.did
from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

delete lr from StateAgencyRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  left join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where (lr.did = 0 or lr.did is null)
      and u.pid is null;

-- What's left?
select count(*)
from StateAgencyRepRepresentation
where did = 0 or did is null;



-- Same hassle for StateConstOfficeRepRepresentation
-- This case is the same as above, but for general public
select count(distinct lr.pid, bd.did)
from StateConstOfficeRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

select count(distinct lr.pid, lr.hid)
from StateConstOfficeRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

update StateConstOfficeRepRepresentation lr
  join Hearing h
    on lr.hid = h.hid
  join BillDiscussion bd
    on bd.hid = h.hid
set lr.did = bd.did
where (lr.did = 0 or lr.did is null)
      and (bd.bid = 'CA_NO BILL DISCUSSED'
           or bd.bid = 'NY_NO BILL DISCUSSED');

-- Samesies for GP
select *
from StateConstOfficeRepRepresentation lr
  left join BillDiscussion bd
  on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
  and bd.did is null;

select distinct lr.hid, lr.state
from StateConstOfficeRepRepresentation lr
  left join BillDiscussion bd
    on lr.hid = bd.hid
where (lr.did = 0 or lr.did is null)
      and bd.did is null;

insert into BillDiscussion
(bid, hid, startVideo, endVideo, numVideos)
  select distinct if(lr.state = 'CA', 'CA_NO BILL DISCUSSED', 'NY_NO BILL DISCUSSED'),
    lr.hid,
    vl.start_video,
    vl.end_video,
    vl.end_video - vl.start_video + 1 as num_vidoes
  from StateConstOfficeRepRepresentation lr
    left join BillDiscussion bd
      on lr.hid = bd.hid
    join video_limits vl
      on lr.hid = vl.hid
  where (lr.did = 0 or lr.did is null)
        and bd.did is null;

-- Just get your last one did's. Don't try and get cute
create TEMPORARY TABLE last_bds
  as
select hid, did
from BillDiscussion
order by lastTouched desc
limit 22;

18285	63

update StateConstOfficeRepRepresentation gp
  join last_bds l
  on gp.hid = l.hid
set gp.did = l.did;

drop view video_limits;

-- Now for GP
select count(distinct lr.pid, u.did)
from StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

select distinct lr.pid, u.did
from StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;


-- smaller number but whatever, we're still gaining data
select count(distinct lr.pid)
from StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

update StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
    join BillDiscussion bd
    on u.did = bd.did
set lr.did = u.did
where lr.did = 0 or lr.did is null;

insert into StateConstOfficeRepRepresentation
(pid, office, position, hid, did, state)
select distinct lr.pid, lr.office, lr.position, lr.hid, u.did, lr.state
from StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where (lr.pid, lr.hid, u.did) not in (select pid, hid, did
                                      from StateConstOfficeRepRepresentation);


-- For GP
select count(*)
from StateConstOfficeRepRepresentation lr
where did = 0 or did is null;

select count(*)
from StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
where did = 0 or did is null;

select count(distinct u.did)
from StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

select distinct u.did
from StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where lr.did = 0 or lr.did is null;

delete lr from StateConstOfficeRepRepresentation lr
  join Video v
    on lr.hid = v.hid
  left join currentUtterance u
    on v.vid = u.vid
       and lr.pid = u.pid
where (lr.did = 0 or lr.did is null)
      and u.pid is null;

-- What's left?
select count(*)
from StateConstOfficeRepRepresentation
where did = 0 or did is null;

