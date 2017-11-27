class Witness(object):
    def __init__(self, session_year, first, last, committee_name, organization_name, bill, session, chamber, hearing_date, position, source, state):
        self.first_name = first
        self.last_name = last
        self.committee = committee_name
        self.hearing_date = hearing_date
        self.position = position.lower()
        self.organization_name = organization_name
        self.source = source
        self.state = state
        self.house = "House" if chamber == "H" else "Senate"
        self.pid = None
        self.bid = "TX_" + str(session_year) + session + bill.split()[0] + bill.split()[1]
        self.cid = None
        self.oid = None
        self.wid = None
