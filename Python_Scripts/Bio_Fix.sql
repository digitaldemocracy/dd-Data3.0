-- Only used to replace the tokens in the database with the correct symbol

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<O-TOKEN>', '�');

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<''-TOKEN>', '''');

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<--TOKEN>', '-');

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<A-TOKEN>', '�');

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<"-TOKEN>', '"');