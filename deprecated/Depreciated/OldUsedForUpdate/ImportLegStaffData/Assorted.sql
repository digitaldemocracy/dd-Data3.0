SELECT p.pid, p.first, p.middle, p.last
FROM Legislator l 
    JOIN Person p
    ON l.pid = p.pid
WHERE l.state = "CA"
ORDER BY p.last;

SELECT p.first, p.middle, p.last, p.pid
FROM Legislator l 
    JOIN Person p
    ON l.pid = p.pid
WHERE l.state = "CA"
    AND last in (SELECT last
                FROM Legislator l 
                    JOIN Person p
                    ON l.pid = p.pid
                WHERE l.state = "CA"
                group by last
                having count(*) > 1);

ALTER TABLE LegislativeStaff
DROP FOREIGN KEY `LegislativeStaff_ibfk_2`,
DROP FOREIGN KEY `LegislativeStaff_ibfk_3`,
DROP COLUMN legislator,
DROP COLUMN committee
DROP COLUMN flag;

DELETE FROM LegislativeStaff;

select *
from LegislativeStaff ls
  join Person p
  on p.pid = ls.pid
where p.last = 'Erke';


select *
from LegOfficePersonnel
where staff_member = 83145;

select *
from LegOfficePersonnel
where end_date is not null;


