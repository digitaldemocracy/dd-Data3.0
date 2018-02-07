from datetime import datetime

class Hearing(object):
    def __init__(self, hearing_date, house, type, state, session_year,  cid, bid, committee_name):
        self.hearing_date = hearing_date
        self.house = house
        self.type = type
        self.state = state
        self.cid = cid
        self.bid = bid
        self.session_year = session_year
        self.committee_name = committee_name
        self.is_active = hearing_date < datetime.today()

