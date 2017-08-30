from bs4 import BeautifulSoup
import requests

class TxHearingPageParser(object):
    def __init__(self):
        self.base_url = "http://www.capitol.state.tx.us"

    def get_chamber_page(self, house):
        return requests.get(self.base_url + "/Committees/Committees.aspx?Chamber=" + house).text

    def create_committee_page_link(self, committee_href):
        return self.base_url + "/Committees/" + committee_href

    def create_attachment_link(self, partial_link):
        return self.base_url + partial_link

    def get_committee_page(self, committee_href):
        return BeautifulSoup(requests.get(self.create_committee_page_link(committee_href)).text, "html.parser")

    def get_house_witness_list(self):
        return self.get_chamber_attachments("H", "witlistmtg")

    def get_senate_witness_list(self):
        return self.get_chamber_attachments("S", "witlistmtg")

    def get_house_minutes(self):
        return self.get_chamber_attachments("H", "minutes")

    def get_senate_minutes(self):
        return self.get_chamber_attachments("S", "minutes")

    def get_house_hearing_notice(self):
        return self.get_chamber_attachments("H", "schedules")

    def get_senate_hearing_notice(self):
        return self.get_chamber_attachments("S", "schedules")

    def get_chamber_attachments(self, chamber, type):
        chamber_soup = BeautifulSoup(self.get_chamber_page(chamber), "html.parser")
        committees = chamber_soup.find_all("a", {"id": "CmteList"})
        links = []
        for committee in committees:
            com_soup = self.get_committee_page(committee["href"])
            all_links = com_soup.find_all("a", {"href": True, "target": True})
            links += [self.create_attachment_link(link["href"]) for link in all_links if link.img and type in link["href"] and "html" in link["href"]]

        return links
