'''
Person model object
- Holds all information regarding the person table for legislators.
Author: Nick Russo
'''
class Person(object):

    '''
    Constructor
    '''
    def __init__(self, first, last, middle, image, source, state, alt_id, pid = None):
        self.first_name = first
        self.last_name = last
        self.middle_name = middle
        self.image = image
        self.source = source
        self.state = state
        self.alt_id = alt_id
        self.pid = pid


    '''
    set_pid
    Used to set pid of person
    '''
    def set_pid(self, pid):
        self.pid = pid


    '''
    person_state_alliation_dict
    Returns a dictionary with the pid and state of the person.
    Used for inserting into person state affiliation.
    '''
    def person_state_affliation_dict(self):
        return {"pid": self.pid,
                "state": self.state}


    '''
    to_dict
    Returns a dictionary representation for the object.
    This is useful for MySQLdb insertions.
    '''
    def to_dict(self):
        return {"first": self.first_name,
                "last": self.last_name,
                "middle": self.middle_name,
                "image": self.image,
                "source": self.source,
                "state": self.state,
                "alt_id": self.alt_id}

