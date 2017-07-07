class Action(object):
    def __init__(self, date, text, seq_num, bid=None):
        self.bid = bid
        self.date = date
        self.text = text
        self.seq_num = seq_num

    def set_bid(self, bid):
        self.bid = bid

    def to_dict(self):
        return {'bid': self.bid,
                'date': self.date,
                'text': self.text,
                'seq_num': self.seq_num}
