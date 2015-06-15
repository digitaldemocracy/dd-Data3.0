INSERT IGNORE INTO DDDB2015Apr.Person(pid, last, first, image)
SELECT pid, last, first, image
FROM DDDB2015.Person;

INSERT IGNORE INTO DDDB2015Apr.Hearing(hid, date)
SELECT hid, date
FROM DDDB2015.Hearing;

INSERT IGNORE INTO DDDB2015Apr.CommitteeHearings(cid, hid)
SELECT cid, hid
FROM DDDB2015.CommitteeHearings;

INSERT IGNORE INTO DDDB2015Apr.Video(vid, youtubeId, hid, position, startOffset, duration)
SELECT vid, youtubeId, hid, position, startOffset, duration
FROM DDDB2015.Video
ON DUPLICATE KEY UPDATE DDDB2015Apr.Video.hid = DDDB2015.Video.hid,
	DDDB2015Apr.Video.position = DDDB2015.Video.position,
	DDDB2015Apr.Video.startOffset = DDDB2015.Video.startOffset,
	DDDB2015Apr.Video.hid = DDDB2015.Video.hid;

INSERT IGNORE INTO DDDB2015Apr.Video_ttml(vid, ttml)
SELECT vid, ttml
FROM DDDB2015.Video_ttml;

INSERT IGNORE INTO DDDB2015Apr.BillDiscussion(did, bid, hid, startVideo, startTime, endVideo, endTime, numVideos)
SELECT did, bid, hid, startVideo, startTime, endVideo, endTime, numVideos
FROM DDDB2015.BillDiscussion;

INSERT IGNORE INTO DDDB2015Apr.attends(pid, hid)
SELECT pid, hid
FROM DDDB2015.attends;

INSERT IGNORE INTO DDDB2015Apr.tag(tid, tag)
SELECT tid, tag
FROM DDDB2015.tag;

INSERT IGNORE INTO DDDB2015Apr.TT_Editor(id, username, password, created, active, role)
SELECT id, username, password, created, active, role
FROM DDDB2015.TT_Editor
ON DUPLICATE KEY UPDATE DDDB2015Apr.TT_Editor.active = DDDB2015.TT_Editor.active;

SET foreign_key_checks = 0;

DELETE FROM DDDB2015Apr.TT_Task;

INSERT IGNORE INTO DDDB2015Apr.TT_Task(tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed)
SELECT tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed
FROM DDDB2015.TT_Task
ON DUPLICATE KEY UPDATE DDDB2015Apr.TT_Task.editor_id = DDDB2015.TT_Task.editor_id, 
	DDDB2015Apr.TT_Task.completed = DDDB2015.TT_Task.completed,
	DDDB2015Apr.TT_Task.assigned = DDDB2015.TT_Task.assigned;

SET foreign_key_checks = 1;

INSERT IGNORE INTO DDDB2015Apr.TT_TaskCompletion(tcid, tid, completion)
SELECT tcid, tid, completion
FROM DDDB2015.TT_TaskCompletion;

DELETE FROM DDDB2015Apr.GeneralPublic;

INSERT INTO DDDB2015Apr.GeneralPublic(pid, affiliation, position, hid)
SELECT pid, affiliation, position, hid
FROM DDDB2015.GeneralPublic;

DELETE FROM DDDB2015Apr.StateAgencyRepRepresentation;

INSERT IGNORE INTO DDDB2015Apr.StateAgencyRepRepresentation(pid, employer, position, hid)
SELECT pid, employer, position, hid
FROM DDDB2015.StateAgencyRep;

DELETE FROM DDDB2015Apr.StateAgencyRep;

INSERT IGNORE INTO DDDB2015Apr.StateAgencyRep(pid, employer, position)
SELECT pid, employer, position
FROM DDDB2015.StateAgencyRep
GROUP BY pid;

DELETE FROM DDDB2015Apr.LegAnalystOfficeRepresentation;

INSERT IGNORE INTO DDDB2015Apr.LegAnalystOfficeRepresentation(pid, hid)
SELECT pid, hid
FROM DDDB2015.LegAnalystOffice;

DELETE FROM DDDB2015Apr.LegAnalystOffice;

INSERT IGNORE INTO DDDB2015Apr.LegAnalystOffice(pid)
SELECT pid
FROM DDDB2015.LegAnalystOffice
GROUP BY pid;

DELETE FROM DDDB2015Apr.LegislativeStaffRepresentation;

INSERT IGNORE INTO DDDB2015Apr.LegislativeStaffRepresentation(pid, flag, legislator, committee, hid)
SELECT pid, flag, legislator, committee, hid
FROM DDDB2015.LegislativeStaff;

DELETE FROM DDDB2015Apr.LegislativeStaff;

INSERT IGNORE INTO DDDB2015Apr.LegislativeStaff(pid, flag, legislator, committee)
SELECT pid, flag, legislator, committee
FROM DDDB2015.LegislativeStaff
GROUP BY pid;

INSERT IGNORE INTO DDDB2015Apr.LobbyistEmployer
SELECT *
FROM DDDB2015.LobbyistEmployer;

INSERT IGNORE INTO DDDB2015Apr.LobbyistRepresentation(pid, le_id, hearing_date, hid)
SELECT pid, le_id, hearing_date, hid
FROM DDDB2015.LobbyistRepresentation;

DELETE FROM DDDB2015Apr.StateConstOfficeRepresentation;

INSERT IGNORE INTO DDDB2015Apr.StateConstOfficeRepresentation(pid, office, position, hid)
SELECT pid, office, position, hid
FROM DDDB2015.StateConstOffice;

DELETE FROM DDDB2015Apr.StateConstOffice;

INSERT IGNORE INTO DDDB2015Apr.StateConstOffice(pid, office, position)
SELECT pid, office, position
FROM DDDB2015.StateConstOffice
GROUP BY pid;