UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<O-TOKEN>', 'ó');

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<''-TOKEN>', '''');

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<--TOKEN>', '-');

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<A-TOKEN>', 'á');

UPDATE DDDB2015Apr.Legislator SET OfficialBio = REPLACE(OfficialBio, '<"-TOKEN>', '"');