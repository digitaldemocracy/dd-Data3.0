delete from LegOfficePersonnel;

delete from OfficePersonnel;

delete from LegislatureOffice;

delete from LegStaffGifts;

delete from LegislativeStaff
where legislator is null and committee is null;   

delete from Term
where year < 2015;

-- really shouldn't be necessary
delete from BillSponsors
where pid not in (select pid
                  from Term);

#ditto
delete from authors
where pid not in (select pid
                  from Term);

delete from Legislator
where pid not in (select pid
                  from Term);





# ScrapeDirectoriesAlternative.py stuff
delete from AndrewTest.LegStaffGifts;
delete from AndrewTest.LegOfficePersonnel;
delete from AndrewTest.LegislativeStaff;
delete from AndrewTest.Legislator
where pid not in (select pid
                  from DDDB2015Dec.Person);
delete from AndrewTest.Term;


insert into AndrewTest.Person
select *
from AndrewTest2.Person
where pid not in (select pid
                  from AndrewTest.Person);


insert into AndrewTest.Legislator
select *
from AndrewTest2.Legislator
where pid not in (select pid
                  from AndrewTest.Legislator);

insert into AndrewTest.Term
select *
from AndrewTest2.Term;

insert into AndrewTest.LegislativeStaff
select *
from AndrewTest2.LegislativeStaff;

insert into AndrewTest.LegStaffGifts
select *
from AndrewTest2.LegStaffGifts;

insert into AndrewTest.LegOfficePersonnel
select *
from AndrewTest2.LegOfficePersonnel;


