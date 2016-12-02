
/*
  Gives the participation for non-legislator speakers in transcribed Hearings
*/
CREATE TABLE SpeakerParticipation (
  pid INT,
  session_year YEAR,
  state VARCHAR(2),
  WordCountTotal FLOAT, -- Total words spoken
  WordCountHearingAvg FLOAT, -- Average words per Hearing
  TimeTotal INT, -- Total time based on length of utterances
  TimeHearingAvg FLOAT, -- Average time per Hearing
  dr_id INT,

  -- fk constraints not enforced, here for convenience
  FOREIGN KEY (pid) REFERENCES Person(pid),
  FOREIGN KEY (session_year, state) REFERENCES Session(start_year, state),
  FOREIGN KEY (state) REFERENCES State(abbrev),
  UNIQUE (dr_id)
);

