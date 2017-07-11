class Version(object):
    def __init__(self, vid, state, bill_state, subject, doctype=None, text=None, date=None, bid=None, url=None):
        self.vid = vid
        self.bid = bid

        self.state = state
        self.date = date

        self.bill_state = bill_state
        self.subject = subject

        self.doctype = doctype
        self.text = text
        self.url = url


    def set_bid(self, bid):
        self.bid = bid

    def set_text(self, text):
        self.text = text

    def set_date(self, date):
        self.date = date

    def to_dict(self):
        return {'vid': self.vid,
                'bid': self.bid,
                'state': self.state,
                'date': self.date,
                'bill_state': self.bill_state,
                'subject': self.subject,
                'doc': self.text}