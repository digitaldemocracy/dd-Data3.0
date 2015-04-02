INSERT INTO DDDB2015Apr.Person(pid, last, first, image)
SELECT pid, last, first, image
FROM DDDB2015.Person;

INSERT INTO DDDB2015Apr.Legislator(pid, description, twitter_handle, capitol_phone, website_url, room_number, email_form_link)
SELECT pid, description, twitter_handle, capitol_phone, website_url, room_number, email_form_link
FROM DDDB2015.Legislator;

INSERT INTO DDDB2015Apr.Term(pid, year, district, house, party, start, end)
SELECT pid, year, district, house, party, start, end
FROM DDDB2015.Term;

INSERT INTO DDDB2015Apr.Committee(cid, house, name)
SELECT cid, house, name
FROM DDDB2015.Committee;

INSERT INTO DDDB2015Apr.servesOn(pid, year, district, house, cid)
SELECT pid, year, district, house, cid
FROM DDDB2015.servesOn;

INSERT INTO DDDB2015Apr.Bill(bid, type, number, state, status, house, session)
SELECT bid, type, number, state, status, house, session
FROM DDDB2015.Bill;

INSERT INTO DDDB2015Apr.Hearing(hid, date)
SELECT hid, date
FROM DDDB2015.Hearing;

INSERT INTO DDDB2015Apr.CommitteeHearings(cid, hid)
SELECT cid, hid
FROM DDDB2015.CommitteeHearings;

INSERT INTO DDDB2015Apr.JobSnapshot(pid, hid, role, employer, client)
SELECT pid, hid, role, employer, client
FROM DDDB2015.JobSnapshot;

INSERT INTO DDDB2015Apr.Action(bid, date, text)
SELECT bid, date, text
FROM DDDB2015.Action;

INSERT INTO DDDB2015Apr.Video(vid, youtubeId, hid, position, startOffset, duration)
SELECT vid, youtubeId, hid, position, startOffset, duration
FROM DDDB2015.Video;

INSERT INTO DDDB2015Apr.Video_ttml(vid, ttml)
SELECT vid, ttml
FROM DDDB2015.Video_ttml;

SELECT 'BillDiscussion' AS '';

INSERT INTO DDDB2015Apr.BillDiscussion(bid, hid, startVideo, startTime, endVideo, endTime, numVideos)
SELECT bid, hid, startVideo, startTime, endVideo, endTime, numVideos
FROM DDDB2015.BillDiscussion;

INSERT INTO DDDB2015Apr.Motion(mid, date, text)
SELECT mid, date, text
FROM DDDB2015.Motion;

INSERT INTO DDDB2015Apr.votesOn(pid, mid, vote)
SELECT pid, mid, vote
FROM DDDB2015.votesOn;

INSERT INTO DDDB2015Apr.BillVersion(vid, bid, date, state, subject, appropriation, substantive_changes, title, digest, text)
SELECT vid, bid, date, state, subject, appropriation, substantive_changes, title, digest, text
FROM DDDB2015.BillVersion;

INSERT INTO DDDB2015Apr.authors(pid, bid, vid, contribution)
SELECT pid, bid, vid, contribution
FROM DDDB2015.authors;

INSERT INTO DDDB2015Apr.attends(pid, hid)
SELECT pid, hid
FROM DDDB2015.attends;

SELECT 'Utterance Issues' AS '';

INSERT INTO DDDB2015Apr.Utterance(uid, vid, pid, time, endTime, text, type, alignment, dataFlag)
SELECT uid, vid, pid, time, endTime, text, type, alignment, dataFlag
FROM DDDB2015.currentUtterance
WHERE NOT EXISTS (
	SELECT 1 FROM DDDB2015Apr.Utterance AS e
	WHERE e.vid = vid 
	AND e.pid = pid 
	AND e.time = time);

UPDATE DDDB2015Apr.Utterance
SET current = 1, finalized = 1;

INSERT INTO DDDB2015Apr.tag(uid, tag)
SELECT tid, tag
FROM DDDB2015.tag
ON DUPLICATE KEY UPDATE tid = tid + 2;

INSERT INTO DDDB2015Apr.Mention(mid, pid)
SELECT mid, pid
FROM DDDB2015.Mention;

INSERT INTO DDDB2015Apr.TT_Editor(id, username, password, created, active, role)
SELECT id, username, password, created, active, role
FROM DDDB2015.TT_Editor;

-- empty
INSERT INTO DDDB2015Apr.TT_Task(tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed)
SELECT tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed
FROM DDDB2015.TT_Task;

INSERT INTO DDDB2015Apr.TT_TaskCompletion(tcid, tid, completion)
SELECT tcid, tid, completion
FROM DDDB2015.TT_TaskCompletion;

INSERT INTO DDDB2015Apr.Lobbyist(pid, filer_id)
SELECT pid, filer_id
FROM DDDB2015.Lobbyist;

INSERT INTO DDDB2015Apr.LobbyingFirm(filer_id, filer_naml, rpt_date, ls_beg_yr, ls_end_yr)
SELECT filer_id, filer_naml, rpt_date, ls_beg_yr, ls_end_yr
FROM DDDB2015.LobbyingFirm;

INSERT INTO DDDB2015Apr.GeneralPublic(pid, affiliation, position, hid)
SELECT pid, affiliation, position, hid
FROM DDDB2015.GeneralPublic;

INSERT INTO DDDB2015Apr.StateAgencyRepRepresentation(pid, employer, position, hid)
SELECT pid, employer, position, hid
FROM DDDB2015.StateAgencyRep;

INSERT INTO DDDB2015Apr.StateAgencyRep(pid, employer, position)
SELECT DISTINCT pid, employer, position
FROM DDDB2015.StateAgencyRep;

INSERT INTO DDDB2015Apr.LegAnalystOfficeRepresentation(pid, hid)
SELECT pid, employer, position, hid
FROM DDDB2015.StateAgencyRep;

INSERT INTO DDDB2015Apr.LegAnalystOffice(pid)
SELECT DISTINCT pid
FROM DDDB2015.LegAnalystOffice;

INSERT INTO DDDB2015Apr.LegislativeStaffRepresentation(pid, flag, legislator, committee, hid)
SELECT pid, flag, legislator, committee, hid
FROM DDDB2015.LegislativeStaff;

INSERT INTO DDDB2015Apr.LegislativeStaff(pid, flag, legislator, committee)
SELECT DISTINCT pid, flag, legislator, committee
FROM DDDB2015.LegislativeStaff;

INSERT INTO DDDB2015Apr.StateConstOfficeRepresentation(pid, office, position, hid)
SELECT pid, office, position, hid
FROM DDDB2015.StateConstOffice;

INSERT INTO DDDB2015Apr.StateConstOffice(pid, office, position)
SELECT DISTINCT pid, office, position
FROM DDDB2015.StateConstOffice;


