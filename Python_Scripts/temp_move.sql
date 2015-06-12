-- used to add committeehearings
-- do not need to use or worry about

CREATE TABLE IF NOT EXISTS CommitteeHearings (
	cid INTEGER,
	hid INTEGER,

	PRIMARY KEY (cid, hid),
	FOREIGN KEY (cid) REFERENCES Committee(cid),
	FOREIGN KEY (hid) REFERENCES Hearing(hid)
);

ALTER TABLE Committee MODIFY COLUMN house ENUM('Senate','Assembly','Joint') NOT NULL;

INSERT INTO CommitteeHearings(hid, cid)
Select hid, cid
From Hearing;

ALTER TABLE Hearing DROP FOREIGN KEY Hearing_ibfk_1;

ALTER TABLE Hearing DROP COLUMN cid;