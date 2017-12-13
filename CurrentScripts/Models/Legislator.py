class Legislator(object):
    def __init__(self, name, image, source, alt_ids,
                 capitol_phone, capitol_fax, website_url, room_number, email,
                 state, year, house, district, party, start, current_term,
                 end = None, caucus = None, pid = None,
                 description = None, twitter_handle = None,
                 email_form_link = None, OfficialBio = None):

        self.set_person_attributes(name, image, source, state, alt_ids, pid)

        self.set_term_attributes(year, house, state, district, party, start, current_term, end, caucus)


        self.capitol_phone = capitol_phone
        self.capitol_fax = capitol_fax
        self.website_url = website_url
        self.room_number = room_number
        self.email = email

        self.description = description
        self.twitter_handle = twitter_handle
        self.email_form_link = email_form_link
        self.official_bio = OfficialBio

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def set_name(self, name):
        self.name = name
        parts = [name["first"],
                 name["nickname"],
                 name["middle"],
                 name["last"],
                 name["suffix"]]
        parts = [part for part in parts if part]
        self.alternate_name = (" ".join(parts)).strip()
        self.first = name["first"] + (" \"" + name["nickname"] + "\"" if name["nickname"] else "")
        self.middle = name["middle"]
        self.last = name["last"]
        self.like_name = name["like_name"]
        self.like_last_name = name["like_last_name"]
        self.like_first_name = name["like_first_name"]
        self.like_nick_name = name["like_nick_name"]
        self.title = name["title"]
        self.suffix = name["suffix"]

    def set_person_attributes(self, name, image, source, state, alt_ids, pid = None):
        self.set_name(name)
        self.image = image
        self.source = source
        self.state = state
        self.alt_ids = alt_ids
        self.current_alt_id = None
        self.pid = pid

    def set_term_attributes(self, year, house, state, district, party, start, current_term, end = None, caucus = None):
        self.year = year
        self.district = district
        self.house = house
        self.party = party
        self.current_term = current_term
        self.start = start
        self.end = end
        self.state = state
        self.caucus = caucus