select count(*)
from Legislator
where state = 'CA';

select count(*)
from Term
where state = 'CA';

select count(*)
from Term
where year = 2015
    and state = 'CA';

select count(*)
from LegislativeStaff;

select count(distinct p.first, p.last)
from LegislativeStaff ls
    join Person p
    on ls.pid = p.pid;


select count(*)
from LegislatureOffice;

select *
from LegislatureOffice
limit 10;

select count(*)
from LegOfficePersonnel;

select count(*)
from LegOfficePersonnel
where end_date is null;

select count(distinct staff_member)
from LegOfficePersonnel;

select count(*)
from OfficePersonnel;

select count(distinct staff_member)
from OfficePersonnel;

select count(*)
from OfficePersonnel
where end_date is null;


select count(*)
from LegislativeStaff 
where pid in (select staff_member
               from LegOfficePersonnel)
    and pid in (select staff_member
                from OfficePersonnel);


select max(start_date)
from LegOfficePersonnel;

select max(start_date)
from OfficePersonnel;

select min(start_date)
from LegOfficePersonnel;

select min(start_date)
from OfficePersonnel;


# should come up empty
select t1.pid, t1.first, t1.middle, t1.last,
    t2.pid, t2.first, t2.middle, t2.last
from (select p1.pid, p1.first, p1.middle, p1.last
      from Legislator l1
          join Person p1
          on l1.pid = p1.pid) t1,
    (select p2.pid, p2.first, p2.middle, p2.last
     from Legislator l2 
        join Person p2
        on l2.pid = p2.pid) t2
where t1.first = t2.first
    and t1.last = t2.last
    and t1.pid != t2.pid;


# should come up empty
select t1.pid, t1.first, t1.middle, t1.last, t1.lastTouched,
    t2.pid, t2.first, t2.middle, t2.last, t2.lastTouched
from (select p1.pid, p1.first, p1.middle, p1.last, l1.lastTouched
      from LegislativeStaff l1
          join Person p1
          on l1.pid = p1.pid) t1,
    (select p2.pid, p2.first, p2.middle, p2.last, l2.lastTouched
     from LegislativeStaff l2 
        join Person p2
        on l2.pid = p2.pid) t2
where t1.first = t2.first
    and t1.last = t2.last
    and (t1.middle = t2.middle or (t1.middle is null 
            and t2.middle is null) )    
    and t1.pid != t2.pid;


# Checks to see if the same staff member has overlapping dates
# Needs to come up empty
select lop1.staff_member, lop1.legislator, lop2.legislator,
    lop1.start_date, lop1.end_date, lop2.start_date, lop2.end_date,
    lop1.house, lop2.house, lop1.term_year, lop2.term_year
from LegOfficePersonnel lop1,
    LegOfficePersonnel lop2
where lop1.staff_member = lop2.staff_member
    and lop1.legislator != lop2.legislator
    and lop1.start_date > lop2.start_date 
    and lop1.start_date < ifnull(lop2.end_date, curdate());

# Checks to see if the same staff member has overlapping dates
# Needs to come up empty
select op1.staff_member, op1.office, op2.office,
    op1.start_date, op1.end_date, op2.start_date, op2.end_date
from OfficePersonnel op1,
    OfficePersonnel op2
where op1.staff_member = op2.staff_member
    and op1.office != op2.office
    and op1.start_date > op2.start_date 
    and op1.start_date < ifnull(op2.end_date, curdate());

# Checks to see if the same staff member has overlapping dates
# Needs to come up empty
select lop1.staff_member, lop1.start_date, lop1.end_date, 
    lop2.start_date, lop2.end_date
from LegOfficePersonnel lop1,
    OfficePersonnel lop2
where lop1.staff_member = lop2.staff_member
    and lop1.start_date > lop2.start_date 
    and lop1.start_date < ifnull(lop2.end_date, curdate());

# opposite of the one above
select lop1.staff_member, lop1.start_date, lop1.end_date, 
    lop2.start_date, lop2.end_date
from LegOfficePersonnel lop1,
    OfficePersonnel lop2
where lop1.staff_member = lop2.staff_member
    and lop1.start_date < lop2.start_date 
    and lop1.start_date > ifnull(lop2.end_date, curdate());



# just looking for reasonable numbers for these three
select count(*)
from LegStaffGifts;

select count(*)
from LegStaffGifts
where legislator is null;

select count(*)
from LegStaffGifts
where legislator is not null;

# Better be empty
select *
from LegStaffGifts
where staff_member is null;


# Gets the number of staff members you had to add for this sheet
select count(distinct staff_member)
from LegStaffGifts
where staff_member not in (select staff_member
                           from LegOfficePersonnel);

# The number of people you matched correctly
select count(distinct staff_member)
from LegStaffGifts
where staff_member in (select staff_member
                           from LegOfficePersonnel);




delete from AndrewTest.Legislator
where pid not in (select pid
                  from DDDB2015Dec.Person);
delete from AndrewTest.Term;
delete from AndrewTest.LegislativeStaff;
delete from AndrewTest.LegStaffGifts;
delete from AndrewTest.LegOfficePersonnel;


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
