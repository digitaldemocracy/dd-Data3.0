## dump the database before you run any of this!!!

drop table if exists CommitteeHearings_temp;
create table CommitteeHearings_temp
  as
select distinct ch.*
from CommitteeHearings ch
  join Hearing h
    on ch.hid = h.hid
  left join HearingAgenda ha
    on h.hid = ha.hid
  left join Video v
    on h.hid = v.hid
where ha.hid is not null
      or v.hid is not null;

select count(*)
  from CommitteeHearings_temp;

select count(*)
from Hearing
where date(lastTouched) < '2017-2-1'

drop table CommitteeHearings;

rename table CommitteeHearings_temp to CommitteeHearings;

describe CommitteeHearings;

alter table CommitteeHearings
    add PRIMARY KEY (cid, hid),
    add FOREIGN KEY (cid) REFERENCES Committee(cid),
    add FOREIGN KEY (hid) REFERENCES Hearing(hid);

delete h
from Hearing h
  left join HearingAgenda ha
    on h.hid = ha.hid
  left join Video v
    on h.hid = v.hid
  left join TT_Task tt
    on tt.hid = h.hid
where ha.hid is null
      and v.hid is null
      and tt.hid is null;