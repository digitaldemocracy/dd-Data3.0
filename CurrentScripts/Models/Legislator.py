class Legislator(object):
    def __init__(self, person, term, state,
                 capitol_phone, capitol_fax, website_url, room_number, email,
                 description = None, twitter_handle = None,
                 email_form_link = None, OfficialBio = None):

        self.person = person
        self.term = term
        self.state = state

        self.capitol_phone = capitol_phone
        self.capitol_fax = capitol_fax
        self.website_url = website_url
        self.room_number = room_number
        self.email = email

        self.description = description
        self.twitter_handle = twitter_handle
        self.email_form_link = email_form_link
        self.official_bio = OfficialBio


    def set_pid(self, pid):
        self.person.set_pid(pid)

    def to_dict(self):
        return {
                "pid": self.person.pid,
                "capitol_phone": self.capitol_phone,
                "capitol_fax": self.capitol_fax,
                "website_url": self.website_url,
                "room_number": self.room_number,
                "email": self.email,
                "description": self.description,
                "twitter_handle": self.twitter_handle,
                "email_form_link": self.email_form_link,
                "OfficialBio": self.official_bio}

    def person_dict(self):
        return self.person.to_dict()

    def term_dict(self):
        return self.term.to_dict()