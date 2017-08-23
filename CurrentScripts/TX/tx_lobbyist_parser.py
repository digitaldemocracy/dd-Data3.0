import re
import os
import csv
import urllib
import openpyxl
import pandas as pd
import datetime as dt
from Models.Lobbyist import Lobbyist
from Utils.Generic_Utils import clean_name

class TxLobbyistParser(object):
    '''
    Purpose: Parses the lobbyist data from the tx ethics website
    Author: Nick Russo
    To the maintainer:
    pls forgive me. I know this is garbage. I know all the regexes are crap. I know that the bad business list
    is terrible. There are a lot of ideas for this parser all of which I do not know if they panned out. So,
    im sorry the lobbyist data is actual garbage. luv u.
    '''
    def __init__(self):
        self.TX_LOBBYIST_URL = 'https://www.ethics.state.tx.us/tedd/2017LobbyistGroupByLobbyist.nopag.xlsx'
        self.LOBBYIST_FILE_DIRECTORY = "./lobbyistFiles/"
        self.TODAYS_LOBBYIST_CSV = self.LOBBYIST_FILE_DIRECTORY + str(dt.date.today()) + "_2017LobbyistGroupByLobbyist.csv"
        self.TODAYS_LOBBYIST_XLSX = self.LOBBYIST_FILE_DIRECTORY + str(dt.date.today()) + "_2017LobbyistGroupByLobbyist.nopag.xlsx"

        self.bad_businesses = ["", "healthcare", "insurance", "8a - 5p", "8-5", "financial services",
                               "state relations", "telecommunications", "education association",
                               "customer operations", "dir government accounts", "sales",
                               "biopharmaceuticals", "telecommunications/cable", "retail grocery",
                               "asset management", "realty investment", "law firm", "law",
                               "land development", "education reform", "mental health policy analysis",
                               "automotive distributor", "mediator/arbitrator", "professional organization",
                               "houston", "association management", "hospital administration",
                               "advocacy and communications", "business development",
                               "health insurance carrier", "legislative affairs", "energy", "retired",
                               "attorney at law", "social worker", "legal and public affairs practice",
                               "association management", "regulatory affairs", "in-house legal counsel",
                               "non-prof public affairs", "marketing", "state account management", "banking",
                               "external affairs", "legal services organization", "governmental affairs",
                               "governmental advocacy","private law practice","financial services",
                               "state advocacy", "rate & financial analysis", "law practice",
                               "self-employed, legislative advocacy and communications",
                               "governmental and regional affairs", "logbbyist", "organizer",
                               "investment management services.", "sole proprietor",
                               "insurance company", "regulatory affairs", "general counsel",
                               "legislative affairs", "labor organization executive",
                               "policy research", "businessman", "principal", "consultant, and novelist",
                               "exec dir government accounts", "state relations executive", "legislative liaison",
                               "law firm", "legislative and regulatory affairs", "advocacy and operations",
                               "governmental relations", "children", "rates & financial analysis",
                               "investment management", "non profit consumer advocacy organization",
                               "insurance", "executive", "health care", "telephony", "foundation",
                               "policy and community engagement", "geologist", "real estate/private property rights",
                               "policy officer of public/private partnerships", "software", "state legislative affairs",
                               "physician services and community relations", "regional customer operations",
                               "public health"]

        self.positions = ["senior vice president", "vice president", "association executive",
                          "ena sr. strategic acct mgr", "program contols manager", "state government affairs"
                          "senior policy associate", "senior legislative counsel"
                          "account executive", "vice president", "association executive",
                          "director of special operations", "executive director", "associate general counsel"
                          "executive director", "president & ceo", "manager", "analyst",
                          "partner ", "vp", "developer", "investment banker", "employee", "government relations",
                          "economist", "lawyer", "government and community relations", "counselor",
                          "police officer", "lobbyist", "president", "ceo", "director", "chief operating officer"]

        self.business_endings = ["llc", "pc", "inc.", "l.p."]

        self.lobbyists_no_client = pd.read_csv("lobbyist.csv").drop_duplicates()
        self.lobbyists_no_client.rename(columns={'\ufeffFilerID': 'FilerID'}, inplace=True)

    def download_files(self):
        """
        Creates a directory, downloads the excel file, and converts it to a csv.
        """
        if not os.path.exists(self.LOBBYIST_FILE_DIRECTORY):
            os.makedirs(self.LOBBYIST_FILE_DIRECTORY)
        if not os.path.exists(self.TODAYS_LOBBYIST_CSV):
            if not os.path.exists(self.TODAYS_LOBBYIST_XLSX):
                urllib.urlretrieve(self.TX_LOBBYIST_URL, self.TODAYS_LOBBYIST_XLSX);
            wb = openpyxl.load_workbook(self.TODAYS_LOBBYIST_XLSX)
            sh = wb.get_active_sheet()
            with open(self.TODAYS_LOBBYIST_CSV, 'wb') as f:
                c = csv.writer(f)
                for r in sh.rows:
                    for cell in r:
                        if type(cell.value) is not unicode:
                            cell.value = unicode(cell.value)

                    c.writerow([cell.value.encode('utf-8') for cell in r if type(cell.value) is unicode])

    def format_name(self, name):
        """
        Converts names with format last suffix, first middle (title) to
        title first middle last suffix
        :param name: the name of a lobbyist from the csv file
        :return: a name formated as title first middle last suffix
        """
        parts = name.split(",")
        last_suffix = parts[0].strip()
        first_middle_title = parts[1].strip().split(" ")
        first_middle = first_middle_title[:-1]
        title = first_middle_title[-1][1:-1]

        return title + " " + " ".join(first_middle) + " " + last_suffix

    def parse_lobbyist(self, row):
        '''
        Taks a row from a pandas dataframe, creates a lobbyist object, and parses/updates the object.
        :param row: A pandas dataframe row
        :return: a lobbyist object
        '''
        if "(" in row["Filer Name"] and ")" in row["Filer Name"]:
            lobbyist = Lobbyist(name = clean_name(row["Filer Name"]),
                                source = self.TX_LOBBYIST_URL,
                                state = "TX",
                                filer_id = row["FilerID"] if not pd.isnull(row["FilerID"]) else None,
                                client_name = row["Client Name"] if not pd.isnull(row["Client Name"]) else None,
                                client_city = row["City.1"] if not pd.isnull(row["City.1"]) else None,
                                client_state = row["State.1"] if not pd.isnull(row["State.1"]) else None,
                                employer_name = row["Business"] if not pd.isnull(row["Business"]) else None,
                                employer_street = row["Addr 1"] if not pd.isnull(row["Addr 1"]) else None,
                                employer_street_2 = row["Addr 2"] if not pd.isnull(row["Addr 2"]) else None,
                                employer_city = row["City"] if not pd.isnull(row["City"]) else None,
                                employer_state = row["State"] if not pd.isnull(row["State"]) else None,
                                report_date = dt.date.today(),
                                employment_start_year = row["Begin"].split("/")[-1] if not pd.isnull(row["Begin"]) else None,
                                employment_end_year = row["Stop"].split("/")[-1] if not pd.isnull(row["Stop"]) else None)
            if not lobbyist.employer_name and "Attn" in lobbyist.employer_street:
                lobbyist.employer_name = lobbyist.employer_street[6:]
                lobbyist.employer_street = lobbyist.employer_street_2
                lobbyist.employer_street_2 = None

            lobbyist.employer_name = self.parse_business(lobbyist.employer_name)
            lookup = self.lobbyists_no_client[self.lobbyists_no_client["FilerID"] == row.FilerID]
            lobbyist.is_direct_employment = self.stats[row["Filer Name"]] == 1

            if len(lookup) == 1 and not pd.isnull(lookup.iloc[0].Business):
                lobbyist.employer_name = lookup.iloc[0].Business
            else:
                lobbyist.employer_name = None
                lobbyist.is_missing_employer = True

            return lobbyist

    def contains_position(self, business_name):
        '''
        Check if a business_name contains a position.
        :param business_name: The business_name field of the lobbyist with client dataframe.
        :return: Boolean if the business_name contains a position in the position list.
        '''
        for position in self.positions:
            if position in business_name.lower():
                return True
        return False

    def is_address(self, business_name):
        '''
        Regex for finding an address in a string.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("((\d.*(rd|freeway|st.))|(([a-z]{2}|[a-z]{2},|texas) \d{5})|(.*(po|p.o.) box.*))", business_name), "Address")

    def is_type_of_consulting(self, business_name):
        '''
        Regex for finding the type of consultant/consulting.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("((.* consultant$)|(consultant$)|(\w+\s+consulting)|(^consulting$)|(consulting,.*))", business_name), "Type of Consultant")

    def is_type_of_lobbyist(self, business_name):
        '''
        Regex for finding the type of lobbyist/lobbying.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("(([a-z]+ lobbyist)|(lobbyist)|(lobby))", business_name), "Type of lobbyist")

    def is_nonprofit_description(self, business_name):
        '''
        Regex for finding a non profit description.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("((.*nonprofit.*)|(.*non-profit.*))", business_name), "Nonprofit")

    def is_job_description(self, business_name):
        '''
        Regex for finding a description of a job.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("((lawyer$)|(lobbyist$)|(consulting$)|(attorney$)|(assistant$))", business_name), "Job Description")

    def is_service_description(self, business_name):
        '''
        Regex for finding a description of a service type.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("(.* (legal|financial|clinical) service.*)", business_name), "Service Description")

    def is_public_affairs_description(self, business_name):
        '''
        Regex for finding a public affairs description.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("(((\w+\s+\w+|\w+,\s+\w+ &) public affairs$)|(^public affairs$))", business_name), "Public Affairs")

    def is_gov_affairs_description(self, business_name):
        '''
        Regex for finding a description of government/governmental affairs.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("((\w+\s+\w+ gov[a-z]+ affairs.*)|(government affairs))", business_name), "Government Affairs")

    def is_political_description(self, business_name):
        '''
        Regex for finding political in the business_name
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("(.*political.*)", business_name), "Political Description")

    def is_provider_description(self, business_name):
        '''
        Regex for finding a description of some sort of provider.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("(.* provider)", business_name), "Provider Description")


    def is_type_of_policy_analyst(self, business_name):
        '''
        Regex for finding a string that contains policy analyst in it.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("((.* policy analyst)|(policy analyst))", business_name), "Policy Analyst")

    def is_type_of_advisor(self, business_name):
        '''
        Regex for finding a description of a type of advisor
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("(.* (advisor|adviser))", business_name), "Advisor")

    def is_type_of_policy(self, business_name):
        '''
        Regex for finding a type of policy.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: Returns the regex search and the reason type of regex.
        NOTE: The second part of the tuple for all of these regex's is to be used in a future feature.
        '''
        return (re.search("(\w+ policy$|^policy$)", business_name), "Policy")

    def is_bad_business_description(self, business_name):
        '''
        Executes all the regex functions. If the regex search group is not null then
        return the tuple.
        :param business_name: The business_name field of the lobbyist dataframe
        :return: (regex search group(what was matched), reason) else None
        '''
        result = self.is_address(business_name)
        if result[0]:
            return result
        result =  self.is_type_of_consulting(business_name)

        if result[0]:
            return result
        result  = self.is_job_description(business_name)

        if result[0]:
            return result
        result = self.is_nonprofit_description(business_name)

        if result[0]:
            return result
        result = self.is_provider_description(business_name)

        if result[0]:
            return result

        result = self.is_public_affairs_description(business_name)
        if result[0]:
            return result

        result = self.is_service_description(business_name)
        if result[0]:
            return result

        result = self.is_gov_affairs_description(business_name)
        if result[0]:
            return result

        result = self.is_type_of_lobbyist(business_name)
        if result[0]:
            return result

        result = self.is_political_description(business_name)
        if result[0]:
            return result

        result = self.is_type_of_policy_analyst(business_name)
        if result[0]:
            return result

        result = self.is_type_of_advisor(business_name)
        if result[0]:
            return result

        result = self.is_type_of_policy(business_name)
        if result[0]:
            return result

        return None

    def split_out_position(self, character, business_name):
        '''
        Remove the position out of a business_name based on the provided character.
        :param character: a character to split on.
        :param business_name: The business field of the lobbyist dataframe
        :return: The business_name without the half containing the position.
        '''
        parts = business_name.split(character)
        result = list()
        for part in parts:
            if not self.contains_position(part):
                result.append(part)
        if len(result) == 0:
            return ""
        return character.join(result).strip() if len(result) > 1 else result[0].strip()

    def remove_company_comma(self, business_name):
        '''
        Removes the last comma from a business_name ex tim, inc.
        NOTE: if there are multiple commas this wont work correctly.
        :param business_name: The business field of the lobbyist dataframe
        :return: The business name without the last comma
        '''
        for ending in self.business_endings:
            if ending in business_name:
                parts = business_name.rsplit(",", 1)
                return "".join(parts)
        return business_name

    def parse_out_position(self, business_name):
        '''
        Removes the position out of a business name ex. executive - walmart, inc
        :param business_name: The business field of the lobbyist dataframe
        :return: The business without the position.
        '''
        split_char = ["- ", ", ", " for ", " at ", " of "]
        business_name = self.remove_company_comma(business_name.lower())
        for char in split_char:
            if char in business_name and self.contains_position(business_name.lower()):
                return self.split_out_position(char.strip(), business_name)
        return business_name

    def contains_company_ending(self, business_name):
        '''
        Checks if the business name contains a business ending (llc etc.)
        :param business_name: The business field of the lobbyist dataframe
        :return: boolean if the name contains a business ending.
        '''
        for ending in self.business_endings:
            if ending in business_name:
                return True
        return False

    def parse_business(self, business_name):
        '''
        Runs all parsing/clean up methods on the business name
        :param business_name: The business field of the lobbyist dataframe
        :return: A parsed and title case business name.
        '''
        if not pd.isnull(business_name):
            business_name = str(business_name)
            business_name = business_name.replace("\n", "").replace("  ", " ").strip()
            business_name = self.parse_out_position(business_name)
            result = self.is_bad_business_description(business_name.lower())
            if (not result or self.contains_company_ending(business_name.lower())) and \
                not self.contains_position(business_name.lower()) and \
                business_name.lower() not in self.bad_businesses:
                return business_name.title()
        return None

    def parse(self):
        '''
        parse all the rows from the lobbyist csv
        :return: a list of lobbyist objects.
        '''
        self.download_files()

        df = pd.read_csv(self.TODAYS_LOBBYIST_CSV)
        df.drop_duplicates(subset=["Filer Name", "Client Name"], inplace=True)
        df.rename(columns={'\ufeffFilerID': 'FilerID'}, inplace=True)

        self.stats = df.groupby("Filer Name").size()

        return [self.parse_lobbyist(row) for index, row in df.iterrows()]
