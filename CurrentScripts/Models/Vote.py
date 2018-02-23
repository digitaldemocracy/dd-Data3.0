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
        self.result = "(PASS)" if result else "(FAIL)"

    def add_vote_detail(self, state, vote_result, pid=None,person=None):
        vote_detail = VoteDetail(state=state,
                                 result=vote_result,
                                 vote=self.vote_id,
                                 pid=pid,
                                 alt_id=person["alt_id"] if person else None,
                                 name = person["name"] if person else None)
        self.vote_details.append(vote_detail)


class VoteDetail(object):
    def __init__(self, state, result, vote=None, pid=None, person=None, alt_id = None, name = None):
        self.state = state
        self.result = result

        self.vote = vote
        self.pid = pid

        self.person = person
        self.alt_id = alt_id
        self.name = name