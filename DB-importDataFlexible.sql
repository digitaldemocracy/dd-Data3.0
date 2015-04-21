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
FROM DDDB2015.Video;

INSERT IGNORE INTO DDDB2015Apr.Video_ttml(vid, ttml)
SELECT vid, ttml
FROM DDDB2015.Video_ttml;

INSERT IGNORE INTO DDDB2015Apr.BillDiscussion(bid, hid, startVideo, startTime, endVideo, endTime, numVideos)
SELECT bid, hid, startVideo, startTime, endVideo, endTime, numVideos
FROM DDDB2015.BillDiscussion;

INSERT IGNORE INTO DDDB2015Apr.attends(pid, hid)
SELECT pid, hid
FROM DDDB2015.attends;

INSERT INTO DDDB2015Apr.Utterance(uid, vid, pid, time, endTime, text, current, finalized, type, alignment, dataFlag)
SELECT uid, vid, pid, time, endTime, text, current, finalized, type, alignment, dataFlag
FROM DDDB2015.Utterance
ON DUPLICATE KEY UPDATE DDDB2015Apr.Utterance.Current = DDDB2015.Utterance.Current, 
	DDDB2015Apr.Utterance.Finalized = DDDB2015.Utterance.Finalized;

INSERT IGNORE INTO DDDB2015Apr.tag(tid, tag)
SELECT tid, tag
FROM DDDB2015.tag
ON DUPLICATE KEY UPDATE tid = tid + 2;

INSERT IGNORE INTO DDDB2015Apr.TT_Editor(id, username, password, created, active, role)
SELECT id, username, password, created, active, role
FROM DDDB2015.TT_Editor;

INSERT IGNORE INTO DDDB2015Apr.TT_Task(tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed)
SELECT tid, hid, did, editor_id, name, vid, startTime, endTime, created, assigned, completed
FROM DDDB2015.TT_Task;

INSERT IGNORE INTO DDDB2015Apr.TT_TaskCompletion(tcid, tid, completion)
SELECT tcid, tid, completion
FROM DDDB2015.TT_TaskCompletion;