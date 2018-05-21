class Version(object):
    def __init__(self, vid, state, bill_state, subject,
                 text=None, title=None, digest=None,
                 date=None, bid=None,
                 appropriation=None, substantive_changes=None,
                 url=None, doctype=None, text_link=None, house = None):
        self.vid = vid
        self.bid = bid

        self.state = state
        self.date = date

        self.bill_state = bill_state
        self.subject = subject
        self.title = title

        self.digest = digest
        self.text = text
        self.text_link = text_link

        self.appropriation = appropriation
        self.substantive_changes = substantive_changes

        self.doctype = doctype
        self.url = url
        self.house = house
