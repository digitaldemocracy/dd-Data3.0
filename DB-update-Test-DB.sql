-- file: DB-update-Test-DB.sql
-- author: Daniel Mangin
-- date: 6/11/2015
-- Description: Used to turn DDDB2015AprTest into an exact copy of DDDB2015Apr

use DDDB2015AprTest;

source DB-clear.sql;
source DB-create.sql;

SET foreign_key_checks = 0;

INSERT INTO servesOn SELECT * FROM DDDB2015Apr.servesOn;
INSERT INTO Bill  SELECT * FROM DDDB2015Apr.Bill ;
INSERT INTO Hearing  SELECT * FROM DDDB2015Apr.Hearing ;
INSERT INTO JobSnapshot  SELECT * FROM DDDB2015Apr.JobSnapshot ;
INSERT INTO Action   SELECT * FROM DDDB2015Apr.Action  ;
INSERT INTO Video  SELECT * FROM DDDB2015Apr.Video ;
INSERT INTO Video_ttml  SELECT * FROM DDDB2015Apr.Video_ttml ;
INSERT INTO BillDiscussion SELECT * FROM DDDB2015Apr.BillDiscussion;
INSERT INTO Committee  SELECT * FROM DDDB2015Apr.Committee ;
INSERT INTO CommitteeHearings SELECT * FROM DDDB2015Apr.CommitteeHearings;
INSERT INTO Lobbyist SELECT * FROM DDDB2015Apr.Lobbyist;
INSERT INTO LobbyistEmployer SELECT * FROM DDDB2015Apr.LobbyistEmployer;
INSERT INTO LobbyistEmployment SELECT * FROM DDDB2015Apr.LobbyistEmployment;
INSERT INTO LobbyistDirectEmployment SELECT * FROM DDDB2015Apr.LobbyistDirectEmployment;
INSERT INTO LobbyingContracts SELECT * FROM DDDB2015Apr.LobbyingContracts;
INSERT INTO LobbyistRepresentation SELECT * FROM DDDB2015Apr.LobbyistRepresentation;
INSERT INTO LegislativeStaff SELECT * FROM DDDB2015Apr.LegislativeStaff;
INSERT INTO LobbyingFirm SELECT * FROM DDDB2015Apr.LobbyingFirm;
INSERT INTO LegAnalystOffice SELECT * FROM DDDB2015Apr.LegAnalystOffice;
INSERT INTO StateAgencyRep SELECT * FROM DDDB2015Apr.StateAgencyRep;
INSERT INTO GeneralPublic SELECT * FROM DDDB2015Apr.GeneralPublic;
INSERT INTO StateConstOffice SELECT * FROM DDDB2015Apr.StateConstOffice;
INSERT INTO Term SELECT * FROM DDDB2015Apr.Term;
INSERT INTO Legislator SELECT * FROM DDDB2015Apr.Legislator;
INSERT INTO Person SELECT * FROM DDDB2015Apr.Person;
INSERT INTO Motion SELECT * FROM DDDB2015Apr.Motion;
INSERT INTO votesOn SELECT * FROM DDDB2015Apr.votesOn;
INSERT INTO BillVersion SELECT * FROM DDDB2015Apr.BillVersion;
INSERT INTO authors SELECT * FROM DDDB2015Apr.authors;
INSERT INTO attends SELECT * FROM DDDB2015Apr.attends;
INSERT INTO Utterance SELECT * FROM DDDB2015Apr.Utterance;
INSERT INTO join_utrtag SELECT * FROM DDDB2015Apr.join_utrtag;
INSERT INTO Mention SELECT * FROM DDDB2015Apr.Mention;
INSERT INTO TT_Editor SELECT * FROM DDDB2015Apr.TT_Editor;
INSERT INTO TT_Task SELECT * FROM DDDB2015Apr.TT_Task;
INSERT INTO TT_TaskCompletion SELECT * FROM DDDB2015Apr.TT_TaskCompletion;
INSERT INTO tag SELECT * FROM DDDB2015Apr.tag;
INSERT INTO user SELECT * FROM DDDB2015Apr.user;
INSERT INTO BillVoteSummary SELECT * FROM DDDB2015Apr.BillVoteSummary;
INSERT INTO BillVoteDetail SELECT * FROM DDDB2015Apr.BillVoteDetail;
INSERT INTO StateConstOfficeRepresentation SELECT * FROM DDDB2015Apr.StateConstOfficeRepresentation;
INSERT INTO StateAgencyRepRepresentation SELECT * FROM DDDB2015Apr.StateAgencyRepRepresentation;
INSERT INTO LegAnalystOfficeRepresentation SELECT * FROM DDDB2015Apr.LegAnalystOfficeRepresentation;
INSERT INTO LegislativeStaffRepresentation SELECT * FROM DDDB2015Apr.LegislativeStaffRepresentation;
INSERT INTO Gift SELECT * FROM DDDB2015Apr.Gift;
INSERT INTO District SELECT * FROM DDDB2015Apr.District;
INSERT INTO Contribution SELECT * FROM DDDB2015Apr.Contribution;

SET foreign_key_checks = 1;

