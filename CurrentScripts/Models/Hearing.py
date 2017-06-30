from datetime import datetime

class Hearing(object):
    def __init__(self, hearing_date, house, type, state, session_year,  cid, bid):
        self.hearing_date = hearing_date
        self.house = house
        self.type = type
        self.state = state
        self.cid = cid
        self.bid = bid
        self.session_year = session_year
        self.is_active = hearing_date < datetime.today()

