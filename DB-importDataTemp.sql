INSERT INTO DDDB2015Apr.Person(pid, last, first, image)
SELECT pid, last, first, image
FROM DDDB2015AprTest.Person;

INSERT INTO DDDB2015Apr.Legislator(pid, description, twitter_handle, capitol_phone, website_url, room_number, email_form_link)
SELECT pid, description, twitter_handle, capitol_phone, website_url, room_number, email_form_link
FROM DDDB2015AprTest.Legislator;

INSERT INTO DDDB2015Apr.Term(pid, year, district, house, party, start, end)
SELECT pid, year, district, house, party, start, end
FROM DDDB2015AprTest.Term;

INSERT INTO DDDB2015Apr.Committee(cid, house, name)
SELECT cid, house, name
FROM DDDB2015AprTest.Committee;

INSERT INTO DDDB2015Apr.servesOn(pid, year, district, house, cid)
SELECT pid, year, district, house, cid
FROM DDDB2015AprTest.servesOn;

INSERT INTO DDDB2015Apr.Bill(bid, type, number, state, status, house, session)
SELECT bid, type, number, state, status, house, session
FROM DDDB2015AprTest.Bill;

INSERT INTO DDDB2015Apr.Hearing(hid, date)
SELECT hid, date
FROM DDDB2015AprTest.Hearing;

INSERT INTO DDDB2015Apr.CommitteeHearings(cid, hid)
SELECT cid, hid
FROM DDDB2015AprTest.CommitteeHearings;

INSERT INTO DDDB2015Apr.JobSnapshot(pid, hid, role, employer, client)
SELECT pid, hid, role, employer, client
FROM DDDB2015AprTest.JobSnapshot;

INSERT INTO DDDB2015Apr.Action(bid, date, text)
SELECT bid, date, text
FROM DDDB2015AprTest.Action;

INSERT INTO DDDB2015Apr.Video(vid, youtubeId, hid, position, startOffset, duration)
SELECT vid, youtubeId, hid, position, startOffset, duration
FROM DDDB2015AprTest.Video;

INSERT INTO DDDB2015Apr.Video_ttml(vid, ttml)
SELECT vid, ttml
FROM DDDB2015AprTest.Video_ttml;

SELECT 'BillDiscussion' AS '';

INSERT INTO DDDB2015Apr.BillDiscussion(bid, hid, startVideo, startTime, endVideo, endTime, numVideos)
SELECT bid, hid, startVideo, startTime, endVideo, endTime, numVideos
FROM DDDB2015AprTest.BillDiscussion;

INSERT INTO DDDB2015Apr.Motion(mid, date, text)
SELECT mid, date, text
FROM DDDB2015AprTest.Motion;

INSERT INTO DDDB2015Apr.votesOn(pid, mid, vote)
SELECT pid, mid, vote
FROM DDDB2015AprTest.votesOn;

INSERT INTO DDDB2015Apr.BillVersion(vid, bid, date, state, subject, appropriation, substantive_changes, title, digest, text)
SELECT vid, bid, date, state, subject, appropriation, substantive_changes, title, digest, text
FROM DDDB2015AprTest.BillVersion;

INSERT INTO DDDB2015Apr.authors(pid, bid, vid, contribution)
SELECT pid, bid, vid, contribution
FROM DDDB2015AprTest.authors;

INSERT INTO DDDB2015Apr.attends(pid, hid)
SELECT pid, hid
FROM DDDB2015AprTest.attends;

SELECT 'Utterance Issues' AS '';

INSERT INTO DDDB2015Apr.Utterance(uid, vid, pid, time, endTime, text, current, finalized, type, alignment, dataFlag)
SELECT uid, vid, pid, time, endTime, text, current, finalized, type, alignment, dataFlag
FROM DDDB2015AprTest.Utterance
WHERE NOT EXISTS (
	SELECT 1 FROM DDDB2015Apr.Utterance AS e
	WHERE e.vid = vid 
	AND e.pid = pid 
	AND e.time = time);

INSERT INTO DDDB2015Apr.tag(tid, tag)
SELECT tid, tag
FROM DDDB2015AprTest.tag
ON DUPLICATE KEY UPDATE tid = tid + 2;

INSERT INTO DDDB2015Apr.Mention(uid, pid)
SELECT uid, pid
FROM DDDB2015AprTest.Mention;

INSERT INTO DDDB2015Apr.TT_Editor(id, username, password, created, active, role)
SELECT id, username, password, created, active, role
FROM DDDB2015AprTest.TT_Editor;

-- empty
INSERT INTO DDDB2015Apr.TT_Task(tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed)
SELECT tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed
FROM DDDB2015AprTest.TT_Task;

INSERT INTO DDDB2015Apr.TT_TaskCompletion(tcid, tid, completion)
SELECT tcid, tid, completion
FROM DDDB2015AprTest.TT_TaskCompletion;

INSERT INTO DDDB2015Apr.Lobbyist(pid, filer_id)
SELECT pid, filer_id
FROM DDDB2015AprTest.Lobbyist;

INSERT INTO DDDB2015Apr.LobbyistEmployer(filer_naml, filer_id, le_id, coalition)
SELECT filer_naml, filer_id, le_id, coalition
FROM DDDB2015AprTest.LobbyistEmployer;

INSERT INTO DDDB2015Apr.LobbyistRepresentation(pid, le_id, hearing_date, hid)
SELECT pid, le_id, hearing_date, hid
FROM DDDB2015AprTest.LobbyistRepresentation;

INSERT INTO DDDB2015Apr.LobbyingFirm(filer_id, filer_naml, rpt_date, ls_beg_yr, ls_end_yr)
SELECT filer_id, filer_naml, rpt_date, ls_beg_yr, ls_end_yr
FROM DDDB2015AprTest.LobbyingFirm;

INSERT INTO DDDB2015Apr.GeneralPublic(pid, affiliation, position, hid)
SELECT pid, affiliation, position, hid
FROM DDDB2015AprTest.GeneralPublic;

INSERT INTO DDDB2015Apr.StateAgencyRepRepresentation(pid, employer, position, hid)
SELECT pid, employer, position, hid
FROM DDDB2015AprTest.StateAgencyRep;

INSERT INTO DDDB2015Apr.StateAgencyRep(pid, employer, position)
SELECT pid, employer, position
FROM DDDB2015AprTest.StateAgencyRep
GROUP BY pid;

INSERT INTO DDDB2015Apr.LegAnalystOfficeRepresentation(pid, hid)
SELECT pid, hid
FROM DDDB2015AprTest.StateAgencyRep;

INSERT INTO DDDB2015Apr.LegAnalystOffice(pid)
SELECT pid
FROM DDDB2015AprTest.LegAnalystOffice
GROUP BY pid;

INSERT INTO DDDB2015Apr.LegislativeStaffRepresentation(pid, flag, legislator, committee, hid)
SELECT pid, flag, legislator, committee, hid
FROM DDDB2015AprTest.LegislativeStaff;

INSERT INTO DDDB2015Apr.LegislativeStaff(pid, flag, legislator, committee)
SELECT pid, flag, legislator, committee
FROM DDDB2015AprTest.LegislativeStaff
GROUP BY pid;

INSERT INTO DDDB2015Apr.StateConstOfficeRepresentation(pid, office, position, hid)
SELECT pid, office, position, hid
FROM DDDB2015AprTest.StateConstOffice;

INSERT INTO DDDB2015Apr.StateConstOffice(pid, office, position)
SELECT pid, office, position
FROM DDDB2015AprTest.StateConstOffice
GROUP BY pid;

INSERT INTO DDDB2015Apr.Gift(RecordId, pid, schedule, sourceName, activity, city, cityState, value, giftDate, reimbursed, giftIncomeFlag, speechFlag, description)
SELECT RecordId, pid, schedule, sourceName, activity, city, cityState, value, giftDate, reimbursed, giftIncomeFlag, speechFlag, description
FROM DDDB2015AorTest.Gift;

