'''
Term model object
- Holds all information regarding Terms for legislators.

Author: Nick Russo

'''
class Term(object):

    '''
    Constructor
    '''
    def __init__(self, person, year, house, state, district, party, start, current_term, end = None, caucus = None):
        self.person = person
        self.year = year
        self.district = district
        self.house = house
        self.party = self.set_party(party)
        self.current_term = current_term
        self.start = start
        self.end = end
        self.state = state
        self.caucus = caucus


    def set_party(self, party):
        if party == "Republican":
            return "Republic"
        elif party == "Democratic":
            return "Democrat"
        else:
             return "Other"


    '''
    to_dict
    Returns a dictionary representation for the object.
    This is useful for MySQLdb insertions.
    '''
    def to_dict(self):
        return {"pid": self.person.pid,
                "year": self.year,
                "district": self.district,
                "house": self.house,
                "party": self.party,
                "start": self.start,
                "end": self.end,
                "state": self.state,
                "caucus": self.caucus}