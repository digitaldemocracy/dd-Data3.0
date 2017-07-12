class Vote(object):
    def __init__(self, vote_date, vote_date_seq,
                 ayes, naes, other, result, motion=None,
                 vote_id=None, bid=None, house=None, cid=None, mid=None):
        self.vote_id = vote_id
        self.bid = bid
        self.cid = cid

        self.mid = mid

        self.vote_date = vote_date
        self.vote_date_seq = vote_date_seq

        self.ayes = ayes
        self.naes = naes
        self.other = other
        self.vote_details = []

        self.house = house

        self.motion = motion
        self.result = result

    def add_vote_detail(self, state, vote_result, pid=None,person=None):
        vote_detail = VoteDetail(state, vote_result, vote=self.vote_id, pid=pid, person=person)
        self.vote_details.append(vote_detail)

    def set_vote_id(self, vote_id):
        self.vote_id = vote_id

    def set_bid(self, bid):
        self.bid = bid

    def set_mid(self, mid):
        self.mid = mid

    def set_cid(self, cid):
        self.cid = cid

    def motion_dict(self):
        return {'mid': self.mid,
                'motion': self.motion,
                'doPass': self.result}

    def to_dict(self):
        return {'bid': self.bid,
                'mid': self.mid,
                'cid': self.cid,
                'date': self.vote_date,
                'vote_seq': self.vote_date_seq,
                'ayes': self.ayes,
                'naes': self.naes,
                'other': self.other,
                'vote_details': self.vote_details,
                'result': self.result}


class VoteDetail(object):
    def __init__(self, state, result, vote=None, pid=None, person=None):
        self.state = state
        self.result = result

        self.vote = vote
        self.pid = pid

        self.person = person

    def set_vote(self, vote):
        self.vote = vote

    def set_pid(self, pid):
        self.pid = pid

    def to_dict(self):
        return {'voteId': self.vote,
                'state': self.state,
                'voteRes': self.result,
                'pid': self.pid}