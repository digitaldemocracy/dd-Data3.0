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

    def set_votes(self, votes):
        self.votes = votes

    def set_versions(self, versions):
        self.versions = versions

    def set_actions(self, actions):
        self.actions = actions

    def to_dict(self):
        return {'bid': self.bid,
                'type': self.bill_type,
                'number': self.number,
                'session': self.session,
                'state': self.state,
                'session_year': self.session_year,
                'billState': self.bill_state,
                'status': self.status,
                'house': self.house}

    def votes_dict(self):
        return [vote.to_dict() for vote in self.votes]

    def versions_dict(self):
        return [version.to_dict() for version in self.versions]

    def actions_dict(self):
        return [action.to_dict() for action in self.actions]