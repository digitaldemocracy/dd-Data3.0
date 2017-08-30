class Lobbyist(object):
    def __init__(self, name, source,
                 state, filer_id,
                 client_name,
                 client_city,
                 client_state,
                 employer_name,
                 employer_street,
                 employer_street_2,
                 employer_city,
                 employer_state,
                 report_date,
                 employment_start_year,
                 employment_end_year):

        self.pid = None
        self.employer_oid = None
        self.client_oid = None

        self.set_name(name)
        self.source = source
        self.state = state
        self.filer_id = filer_id

        self.employer_name = employer_name
        self.employer_street = employer_street
        self.employer_street_2 = employer_street_2
        self.employer_city = employer_city
        self.employer_state = employer_state

        self.is_direct_employment = None
        self.is_missing_employer = None


        self.client_name = client_name
        self.client_city = client_city
        self.client_state = client_state

        self.report_date = report_date
        self.employment_start_year = employment_start_year
        self.employment_end_year = employment_end_year



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

    def employer_dict(self):
        return {"name" : self.employer_name, "state" : self.employer_state, "source" : self.source}

    def client_dict(self):
        return {"name" : self.client_name, "state" : self.client_state, "source" : self.source}
