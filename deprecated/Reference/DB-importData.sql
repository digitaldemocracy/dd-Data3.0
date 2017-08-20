INSERT INTO DDDB2015AprTest.Person(pid, last, first, image)
SELECT pid, last, first, image
FROM DDDB2015.Person;

INSERT INTO DDDB2015AprTest.Legislator(pid, description, twitter_handle, capitol_phone, website_url, room_number, email_form_link)
SELECT pid, description, twitter_handle, capitol_phone, website_url, room_number, email_form_link
FROM DDDB2015.Legislator;

INSERT INTO DDDB2015AprTest.Term(pid, year, district, house, party, start, end)
SELECT pid, year, district, house, party, start, end
FROM DDDB2015.Term;

INSERT INTO DDDB2015AprTest.Committee(cid, house, name)
SELECT cid, house, name
FROM DDDB2015.Committee;

INSERT INTO DDDB2015AprTest.servesOn(pid, year, district, house, cid)
SELECT pid, year, district, house, cid
FROM DDDB2015.servesOn;

INSERT INTO DDDB2015AprTest.Bill(bid, type, number, state, status, house, session)
SELECT bid, type, number, state, status, house, session
FROM DDDB2015.Bill;

INSERT INTO DDDB2015AprTest.Hearing(hid, date)
SELECT hid, date
FROM DDDB2015.Hearing;

INSERT INTO DDDB2015AprTest.CommitteeHearings(cid, hid)
SELECT cid, hid
FROM DDDB2015.CommitteeHearings;

INSERT INTO DDDB2015AprTest.JobSnapshot(pid, hid, role, employer, client)
SELECT pid, hid, role, employer, client
FROM DDDB2015.JobSnapshot;

INSERT INTO DDDB2015AprTest.Action(bid, date, text)
SELECT bid, date, text
FROM DDDB2015.Action;

INSERT INTO DDDB2015AprTest.Video(vid, youtubeId, hid, position, startOffset, duration)
SELECT vid, youtubeId, hid, position, startOffset, duration
FROM DDDB2015.Video;

INSERT INTO DDDB2015AprTest.Video_ttml(vid, ttml)
SELECT vid, ttml
FROM DDDB2015.Video_ttml;

SELECT 'BillDiscussion' AS '';

INSERT INTO DDDB2015AprTest.BillDiscussion(bid, hid, startVideo, startTime, endVideo, endTime, numVideos)
SELECT bid, hid, startVideo, startTime, endVideo, endTime, numVideos
FROM DDDB2015.BillDiscussion;

INSERT INTO DDDB2015AprTest.Motion(mid, date, text)
SELECT mid, date, text
FROM DDDB2015.Motion;

INSERT INTO DDDB2015AprTest.votesOn(pid, mid, vote)
SELECT pid, mid, vote
FROM DDDB2015.votesOn;

INSERT INTO DDDB2015AprTest.BillVersion(vid, bid, date, state, subject, appropriation, substantive_changes, title, digest, text)
SELECT vid, bid, date, state, subject, appropriation, substantive_changes, title, digest, text
FROM DDDB2015.BillVersion;

INSERT INTO DDDB2015AprTest.authors(pid, bid, vid, contribution)
SELECT pid, bid, vid, contribution
FROM DDDB2015.authors;

INSERT INTO DDDB2015AprTest.attends(pid, hid)
SELECT pid, hid
FROM DDDB2015.attends;

SELECT 'Utterance Issues' AS '';

INSERT INTO DDDB2015AprTest.Utterance(uid, vid, pid, time, endTime, text, current, finalized, type, alignment, dataFlag)
SELECT uid, vid, pid, time, endTime, text, current, finalized, type, alignment, dataFlag
FROM DDDB2015.Utterance
WHERE NOT EXISTS (
	SELECT 1 FROM DDDB2015AprTest.Utterance AS e
	WHERE e.vid = vid 
	AND e.pid = pid 
	AND e.time = time);

INSERT INTO DDDB2015AprTest.tag(tid, tag)
SELECT tid, tag
FROM DDDB2015.tag
ON DUPLICATE KEY UPDATE tid = tid + 2;

INSERT INTO DDDB2015AprTest.Mention(uid, pid)
SELECT uid, pid
FROM DDDB2015.Mention;

INSERT INTO DDDB2015AprTest.TT_Editor(id, username, password, created, active, role)
SELECT id, username, password, created, active, role
FROM DDDB2015.TT_Editor;

-- empty
INSERT INTO DDDB2015AprTest.TT_Task(tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed)
SELECT tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed
FROM DDDB2015.TT_Task;

INSERT INTO DDDB2015AprTest.TT_TaskCompletion(tcid, tid, completion)
SELECT tcid, tid, completion
FROM DDDB2015.TT_TaskCompletion;

INSERT INTO DDDB2015AprTest.Lobbyist(pid, filer_id)
SELECT pid, filer_id
FROM DDDB2015.Lobbyist;

INSERT INTO DDDB2015AprTest.LobbyistEmployer(filer_naml, filer_id, le_id, coalition)
SELECT filer_naml, filer_id, le_id, coalition
FROM DDDB2015.LobbyistEmployer;

INSERT INTO DDDB2015AprTest.LobbyistRepresentation(pid, le_id, hearing_date, hid)
SELECT pid, le_id, hearing_date, hid
FROM DDDB2015.LobbyistRepresentation;

INSERT INTO DDDB2015AprTest.LobbyingFirm(filer_id, filer_naml, rpt_date, ls_beg_yr, ls_end_yr)
SELECT filer_id, filer_naml, rpt_date, ls_beg_yr, ls_end_yr
FROM DDDB2015.LobbyingFirm;

INSERT INTO DDDB2015AprTest.GeneralPublic(pid, affiliation, position, hid)
SELECT pid, affiliation, position, hid
FROM DDDB2015.GeneralPublic;

INSERT INTO DDDB2015AprTest.StateAgencyRepRepresentation(pid, employer, position, hid)
SELECT pid, employer, position, hid
FROM DDDB2015.StateAgencyRep;

INSERT INTO DDDB2015AprTest.StateAgencyRep(pid, employer, position)
SELECT pid, employer, position
FROM DDDB2015.StateAgencyRep
GROUP BY pid;

INSERT INTO DDDB2015AprTest.LegAnalystOfficeRepresentation(pid, hid)
SELECT pid, hid
FROM DDDB2015.StateAgencyRep;

INSERT INTO DDDB2015AprTest.LegAnalystOffice(pid)
SELECT pid
FROM DDDB2015.LegAnalystOffice
GROUP BY pid;

INSERT INTO DDDB2015AprTest.LegislativeStaffRepresentation(pid, flag, legislator, committee, hid)
SELECT pid, flag, legislator, committee, hid
FROM DDDB2015.LegislativeStaff;

INSERT INTO DDDB2015AprTest.LegislativeStaff(pid, flag, legislator, committee)
SELECT pid, flag, legislator, committee
FROM DDDB2015.LegislativeStaff
GROUP BY pid;

INSERT INTO DDDB2015AprTest.StateConstOfficeRepresentation(pid, office, position, hid)
SELECT pid, office, position, hid
FROM DDDB2015.StateConstOffice;

INSERT INTO DDDB2015AprTest.StateConstOffice(pid, office, position)
SELECT pid, office, position
FROM DDDB2015.StateConstOffice
GROUP BY pid;


