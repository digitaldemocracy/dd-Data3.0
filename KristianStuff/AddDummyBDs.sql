select *
from Hearing h
  left join BillDiscussion bd
  on h.hid = bd.hid
where bd.did is null;

update Utterance u
  join Video v
  on u.vid = v.vid
  join Hearing h
  on v.hid = h.hid
  join BillDiscussion bd
  on h.hid = bd.hid
set u.did = bd.did
where u.current = True
  and u.finalized = True
  and u.did is null;

select distinct bd.did
from Utterance u
  join Video v
    on u.vid = v.vid
  join Hearing h
    on v.hid = h.hid
  join BillDiscussion bd
  on bd.hid = h.hid
where u.current = True
      and u.finalized = True
#         and h.hid = 'Informational'
      and h.hid = 1820;

