ALTER TABLE Bill
    ADD visibility_flag BOOLEAN DEFAULT 0;




-- Updating the Term to have Bio
ALTER TABLE Term
    ADD official_bio TEXT;

UPDATE Term as t
    join Legislator l
    on t.pid = l.pid
set t.official_bio = l.OfficialBio;

alter table Legislator
    drop column OfficialBio;


# Updates to Bill
ALTER TABLE Bill
    ADD year YEAR;

update Bill
    set year = SessionYear;

update Bill
    set SessionYear = 2015
where SessionYear is not null;