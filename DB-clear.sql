-- file: DB-clear.sql
-- author: Daniel Mangin
-- date: 6/11/2015
-- Description: Used to drop all of the tables in the current database
-- note: this will only work on the currently used database

SET foreign_key_checks = 0;

DROP TABLE servesOn;
DROP TABLE Bill; 
DROP TABLE Hearing; 
DROP TABLE JobSnapshot; 
DROP TABLE Action;  
DROP TABLE Video; 
DROP TABLE Video_ttml; 
DROP TABLE BillDiscussion;
DROP TABLE Committee; 
DROP TABLE CommitteeHearings;
DROP TABLE Lobbyist;
DROP TABLE LobbyistEmployer;
DROP TABLE LobbyistEmployment;
DROP TABLE LobbyistDirectEmployment;
DROP TABLE LobbyingContracts;
DROP TABLE LobbyistRepresentation;
DROP TABLE LegislativeStaff;
DROP TABLE LobbyingFirm;
DROP TABLE LegAnalystOffice;
DROP TABLE StateAgencyRep;
DROP TABLE GeneralPublic;
DROP TABLE StateConstOffice;
DROP TABLE Term;
DROP TABLE Legislator;
DROP TABLE Person;
DROP TABLE Motion;
DROP TABLE votesOn;
DROP TABLE BillVersion;
DROP TABLE authors;
DROP TABLE attends;
DROP VIEW currentUtterance;
DROP TABLE Utterance;
DROP TABLE join_utrtag;
DROP TABLE Mention;
DROP TABLE TT_Editor;
DROP TABLE TT_Task;
DROP TABLE TT_TaskCompletion;
DROP TABLE tag;
DROP TABLE user;
DROP TABLE BillVoteSummary;
DROP TABLE BillVoteDetail;
DROP TABLE StateConstOfficeRepresentation;
DROP TABLE StateAgencyRepRepresentation;
DROP TABLE LegAnalystOfficeRepresentation;
DROP TABLE LegislativeStaffRepresentation;
DROP TABLE Gift;
DROP TABLE District;
DROP TABLE Contribution;

SET foreign_key_checks = 1;
