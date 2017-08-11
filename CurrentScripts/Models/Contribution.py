class Contribution(object):
    def __init__(self, first_name, last_name, donor_name, amount,
                 state, date, year,
                 pid=None, house=None, donor_org=None, oid=None):
        self.first_name = first_name
        self.last_name = last_name

        self.donor_name = donor_name
        self.amount = amount

        self.state = state

        self.date = date
        self.year = year

        self.pid = pid
        self.house = house

        self.donor_org = donor_org
        self.oid = oid

        self.contribution_id = None

    def set_pid(self, pid):
        self.pid = pid

    def set_house(self, house):
        self.house = house

    def set_oid(self, oid):
        self.oid = oid

    def set_id(self, contribution_id):
        self.contribution_id = contribution_id
