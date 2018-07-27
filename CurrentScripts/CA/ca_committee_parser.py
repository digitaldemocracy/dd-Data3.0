'''
ca_committee_parser
Author: Nick Russo
Purpose: Web scrapes assembly and senate websites to get committee names and committee website links.
         Then web scrapes each committee website for committee member information.
Python3 conversion notes: urllib2 replaced with urllib
'''

from urllib.request import urlopen, HTTPError
from bs4 import BeautifulSoup
from Models.Committee import *
from Utils.Generic_Utils import *
from Models.CommitteeMember import *
from Utils.Generic_Utils import format_committee_name

class CaCommitteeParser(object):

    def __init__(self, session_year, leg_session_year):
        self.session_year = session_year
        self.leg_session_year = leg_session_year
        self.state = "CA"

    def format_link(self, link, house_link):
        '''
        In special cases, mostly select committees, the committee link
        is just /committeename. This method formats the link.
        :param link: link given in the href of the html
        :param house_link: the link of the committees page for the house
        :return: a properly formatted link
        '''
        if link.strip().startswith("/"):
            link = house_link.replace("/committees", link.strip())
        return link.strip().rstrip("/")

    def remove_position(self, member_info):
        '''
        Removes to the position and alternate indicators in the members name
        :param member_info: the raw text from the html tag
                            usually first last position (if applicable)
        :return: The name without the position.
        '''
        return member_info.replace("(Co Chair)", "") \
            .replace("Co-Chair", "") \
            .replace("(Vice Chair)", "") \
            .replace("(Chair)", "") \
            .replace(" (Dem. Alternate)", "") \
            .replace(" (Rep. Alternate)", "") \
            .replace(", (Democratic Alternate)", "") \
            .replace(", (Republican Alternate)", "") \
            .replace(", Dem. Alternate", "") \
            .replace(", Rep. Alternate", "") \
            .replace(", Democratic Alternate", "") \
            .replace(", Republican Alternate", "") \
            .replace("(D)", "") \
            .replace("(R)", "") \
            .replace("Contact", "") \
            .replace("Senator", "") \
            .replace("Assembly Member", "") \
            .strip()

    def get_district(self, member_link, member_info):
        '''
        Parses the district out of the link given to the home page of the legislator.
        Examples: sd10.ca.senate.gov or ca.senate.gov/ad10 or crazylongurl.com/blah=?A10othercrapa
        :param member_link: The link to the home page of a legislator.
        :param member_info: Information about the legislator. Usually Senator Brady Haran
        :return: A tuple containing the house and district. If the district is not available,
                 district is None else both are None.
        '''
        district = re.search("(sd|ad|a)([0-9]{2})", member_link.lower())
        if district:
            return ("Senate" if district.group(1) == "sd" else "Assembly", district.group(2))

        # Should only reach here if the senator / assembly member has a unique url that
        # does not contain a district. Need to try to find at least a house
        # EX http://nielsen.cssrc.us/
        house = re.search("(Assembly Member|Senator)", member_info)
        if house:
            return ("Senate" if house.group(0) == "Senator" else "Assembly", None)

        return (None, None)

    def get_position(self, member_info):
        '''
        Formats the committee position based on the DB enum.
        :param member_info: Raw text in the html tag which
                            usually is FirstName Lastname Position(if applicable)
        :return: The formatted position
        '''
        if "(Co Chair)" in member_info:
            return "Co-Chair"
        if "(Vice Chair)" in member_info:
            return "Vice-Chair"
        elif "(Chair)" in member_info:
            return "Chair"
        else:
            return "Member"

    def format_name(self, name):
        '''
        Removes the house title and other items from a legislators name.
        ex Senator Andres Iniesta [pdf] --> Andres Iniesta
        :param name: The name of the legislator with their title.
        :return: The name of the legislator without their title.
        '''
        return clean_name(self.remove_position(name.replace("[pdf]", "") \
                                         .replace(u"\xa0", u" ") \
                                         .strip()))

    def get_members(self, committee_link, type, house):
        '''
        Parses the members from the committee members page for all committee types
        and returns a list of committee member objects
        :param committee_link: The link to the committee home page
        :param type: Committee type: Standing Committee, Sub, Select Committee, Joint
        :return: list of CommitteeMember objects
        '''
        page = self.open_committe_link(committee_link, type)
        htmlSoup = BeautifulSoup(page, "html.parser")

        members_formatted = list()
        members_raw = htmlSoup.find_all("a", text=re.compile(r'(Senator .*)|(Assembly Member.*)'))
        for member in members_raw:

            # Senator Harris and Senator Feinstein have [pdf] in there name.
            if "[pdf]" not in member.text.lower():
                name = self.format_name(member.text)
                position = self.get_position(member.text)
                houseAndDistrict = self.get_district(member['href'], member.text)

                cm = CommitteeMember(name=name,
                                     position=position,
                                     state=self.state,
                                     session_year=self.session_year,
                                     leg_session_year=self.leg_session_year,
                                     current_flag=1,
                                     house=houseAndDistrict[0],
                                     district=houseAndDistrict[1])
                members_formatted.append(cm)

        return members_formatted

    def open_committe_link(self, committee_link, type):
        '''
        committees have several types of links for the committee members.
        This method tries opening the three different kinds.
        :param committee_link: Link to the committees home page
        :return: The raw html of the committee members page.
        '''

        if (type == "Standing" or type == "Select" or type == "Joint") \
                and "assembly.ca.gov" in committee_link:
            try:
                host = committee_link + "/membersstaff"
                return urlopen(host).read().decode("utf-8")
            except HTTPError:
                pass

            try:
                host = committee_link + "/content/members"
                return urlopen(host).read().decode("utf-8")
            except HTTPError:
                pass

            try:
                host = committee_link + "/content/members-staff"
                return urlopen(host).read().decode("utf-8")
            except HTTPError:
                pass
        elif type == "Joint" and \
                ("legislature.ca.gov" in committee_link or "assembly.ca.gov" in committee_link):
            try:
                host = committee_link + "/members"
                return urlopen(host).read().decode("utf-8")
            except HTTPError:
                pass

        return urlopen(committee_link).read().decode("utf-8")

    def format_house(self, type, house):
        return "Joint" if type.lower() == "joint" else house.title()

    def format_committee_type(self, name, section):
        if "sub" in name.lower():
            if "budget" in name.lower():
                return "Budget Subcommittee"
            return "Subcommittee"
        return section

    def get_committees(self, house):
        '''
        Parse all of the committees from the specified house's home page
        then fills the committe with its members.
        :param house: senate or assembly
        :return: A list of Committee objects with members.
        '''
        host = "http://%s.ca.gov/committees" % house.lower()
        htmlSoup = BeautifulSoup(urlopen(host).read(), "html.parser")
        committee_type_section = None
        sub_committee_parent = ""
        committees_block = htmlSoup.find_all(["h2", "h3", "span"])
        committee_list = list()
        for row in committees_block:
            if row.name == "h2":
                committee_type_section = row.text.replace("Committees", "").strip()
            elif row.name == "h3":
                sub_committee_parent = row.text
            elif row.a is not None:
                link = self.format_link(row.a["href"], host)
                if ".gov" in link:
                    committee_type = self.format_committee_type(row.a.text, committee_type_section)
                    short_name = row.a.text.split(" on ")[1].strip() if "Joint Committee on" in row.a.text \
                                                                                          or "sub" in committee_type.lower() \
                                                                                          or "Special Committee on" in row.a.text \
                                                                                          else row.a.text
                    full_name = format_committee_name(row.a.text,
                                                      house,
                                                      committee_type,
                                                      sub_committee_parent if "sub" in committee_type.lower() else None)
                    committee = Committee(type=committee_type,
                                          name=full_name,
                                          short_name=short_name.strip("the").strip(),
                                          link=link,
                                          house=self.format_house(committee_type, house),
                                          state=self.state,
                                          session_year=self.session_year,
                                          members=self.get_members(link, committee_type, house))

                    committee_list.append(committee)

        return committee_list

    def get_committee_list(self):
        comms = self.get_committees("senate")
        comms += self.get_committees("assembly")
        return comms