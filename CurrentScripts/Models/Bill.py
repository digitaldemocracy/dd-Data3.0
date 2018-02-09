class Bill(object):
    def __init__(self, bid, bill_type, number, house, bill_state,
                 session, state, session_year, title=None, os_bid = None, status=None):

        self.bid = bid

        self.votes = None
        self.versions = None
        self.actions = None

        self.bill_type = bill_type
        self.number = number

        self.session = session
        self.state = state
        self.house = house
        self.session_year = session_year

        self.bill_state = bill_state
        self.status = status

        self.os_bid = os_bid
        self.title = title