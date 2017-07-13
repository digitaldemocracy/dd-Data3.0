class Version(object):
    def __init__(self, vid, state, bill_state, subject,
                 text=None, title=None, digest=None,
                 date=None, bid=None,
                 appropriation=None, substantive_changes=None,
                 url=None, doctype=None):
        self.vid = vid
        self.bid = bid

        self.state = state
        self.date = date

        self.bill_state = bill_state
        self.subject = subject
        self.title = title

        self.digest = digest
        self.text = text
        self.text_link = None

        self.appropriation = appropriation
        self.substantive_changes = substantive_changes

        self.doctype = doctype
        self.url = url


    def set_bid(self, bid):
        self.bid = bid

    def set_text(self, text):
        self.text = text

    def set_text_link(self, text_link):
        self.text_link = text_link

    def set_date(self, date):
        self.date = date

    def to_dict(self):
        return {'vid': self.vid,
                'bid': self.bid,
                'state': self.state,
                'date': self.date,
                'bill_state': self.bill_state,
                'subject': self.subject,
                'appropriation': self.appropriation,
                'substantive_changes': self.substantive_changes,
                'title': self.title,
                'digest': self.digest,
                'doc': self.text,
                'text_link': self.text_link}