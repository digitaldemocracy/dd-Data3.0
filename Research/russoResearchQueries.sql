
# Gives Version number is the legislator a sponsor.
DROP FUNCTION IF EXISTS F_TEST;
DELIMITER //
CREATE FUNCTION F_TEST(a VARCHAR(33), b VARCHAR(23)) RETURNS INTEGER
BEGIN
    DECLARE result INTEGER DEFAULT -1;
    DECLARE row_number INTEGER DEFAULT -1;
    SET @row_number = 0;
    select num into result
        from (select (@row_number:=@row_number + 1) as num, vid
              from BillVersion
             where bid = b
             order by date) as nums
        where nums.vid = a;
    RETURN result;
END //
DELIMITER ;
select pid, F_TEST(vid, bid) as version_num, vid, bid from BillSponsors;

# Total number of utterance on a bill. DANGER TAKES FOREVER.
select bd.bid, count(*) as total_utterances
from (select * from Utterance where bid like "CA_20152016%" order by lastTouched desc limit 10000) as u, BillDiscussion bd
where u.did = bd.did
group by bd.bid

# Total number of utterances on bill who are on committee
select bd.bid, count(*) as total_utterances
from BillDiscussion bd, (select *
                         from Utterance where bid like "CA_20152016%"
                         order by lastTouched desc
                         limit 100) as u,
                        CommitteeHearings ch,
                        servesOn s

where u.did = bd.did
and bd.hid = ch.hid
and ch.cid = s.cid
and s.pid = u.pid
group by bd.bid;

# Total number of utterances on bill who are not on committee
select bd.bid, count(*) as total_utterances
from BillDiscussion bd, (select *
                         from Utterance where bid like "CA_20152016%"
                         order by lastTouched desc
                         limit 100) as u,
                        CommitteeHearings ch
where u.did = bd.did
and bd.hid = ch.hid
and u.pid not in (select * from servesOn s where s.cid = ch.cid)
group by bd.bid;
