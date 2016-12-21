-- MYSQL Table Chnages:
-- 
--     - Alter:
--         - Contribution
--             - Industry type
--         - Gift
--             - Industry type
--         - Behests
--             - Industry type
-- 
--     - New:
--         - DistrictCensus
--             - Identifier (house, district, state, year)
--                 - house (either assembly or senate)
--                 - district number
--                 - state (Ca)
--                 - year
--             - Category (attribute/field name)
--                 - Demographics (age, ethnicity etc..)
--                 - Employment
--                 - Medical
--                 - maybe housing? 
--             - Value
--                 - Numbers in those fields
-- 
--     Example:
--         house, district, state, year, population of Latinos, 444444


ALTER TABLE Contribution ADD industryType varchar(200);
ALTER TABLE Gift ADD industryType varchar(200);
ALTER TABLE Behests ADD industryType varchar(200);

CREATE TABLE IF NOT EXISTS DistrictCensus (
    state varchar(2),
    house varchar(200),
    did INTEGER,
    year INTEGER,
    attribute varchar(200),
    value DOUBLE,
    type ENUM('Latino_Demo', 'NonLatino_Demo', 'Age', 'Medical', 'Nativity', 'Employment'),
    lastTouched TIMESTAMP DEFAULT NOW() ON UPDATE NOW(),

    FOREIGN KEY (state, house, did, year) REFERENCES District(state, house, did, year)
)
ENGINE = INNODB
CHARACTER SET utf8 COLLATE utf8_general_ci;