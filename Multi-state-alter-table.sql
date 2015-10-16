CREATE TABLE IF NOT EXISTS State (
  abbrev VARCHAR(2),  -- eg CA, AZ
  country VARCHAR(200), -- eg United States
  name VARCHAR(200), -- eg California, Arizona

    PRIMARY KEY (abbrev)
);

INSERT INTO State
(abbrev, country, name)
VALUES 
("CA", "United States", "California");


CREATE TABLE IF NOT EXISTS House (
  name VARCHAR(200), -- Name for the house. eg Assembly, Senate
  state VARCHAR(2),

  PRIMARY KEY (name, state),
  FOREIGN KEY (state) REFERENCES State(abbrev)
  ); 

INSERT INTO House
(name, state)
VALUES
("Assembly", "CA"),
("Senate", "CA");

ALTER TABLE servesOn
    CHANGE house house VARCHAR(200),
    CHANGE state state VARCHAR(2),
    ADD FOREIGN KEY (house, state) REFERENCES House(house, state),
    DROP PRIMARY KEY, 
    ADD PRIMARY (pid, year, house, state, cid),
    DROP district;

ALTER TABLE Term 
    CHANGE house house VARCHAR(200),
    ADD state VARCHAR(2),
    ADD caucus VARCHAR(200),
    ADD FOREIGN KEY (house, state) REFERENCES House(house, state);
