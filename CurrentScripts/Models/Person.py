'''
Person model object
- Holds all information regarding the person table for legislators.
Author: Nick Russo
'''
class Person(object):

    '''
    Constructor
    '''
    def __init__(self, name, image, source, state, alt_id, pid = None):
        self.name = name
        parts = [name["first"],
                 name["nickname"],
                 name["middle"],
                 name["last"],
                 name["suffix"]]
        parts = [part for part in parts if part]
        self.alternate_name = (" ".join(parts)).strip()
        self.first = name["first"] + ( " \"" + name["nickname"] + "\"" if name["nickname"] else "")
        self.middle = name["middle"]
        self.last = name["last"]
        self.like_name = name["like_name"]
        self.like_last_name = name["like_last_name"]
        self.like_first_name = name["like_first_name"]
        self.like_nick_name = name["like_nick_name"]
        self.title = name["title"]
        self.suffix = name["suffix"]
        self.house = None
        self.district = None

        self.image = image
        self.source = source
        self.state = state
        self.alt_id = alt_id
        self.pid = pid


    '''
    person_state_affiliation_dict
    Returns a dictionary with the pid and state of the person.
    Used for inserting into person state affiliation.
    '''
    def person_state_affiliation_dict(self):
        return {"pid": self.pid,
                "state": self.state}
