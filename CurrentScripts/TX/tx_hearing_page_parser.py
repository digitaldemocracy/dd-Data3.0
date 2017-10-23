#!/usr/bin/env

from bs4 import BeautifulSoup
import requests
from Models.Witness import *


# Parsing class for the TX hearing page using Meetings by Committee
# Source: http://www.capitol.state.tx.us/MnuCommittees.aspx
class TxHearingPageParser(object):
    def __init__(self, session_year):
        self.broken_line = None
        self.counter = 0
        self.base_url = "http://www.capitol.state.tx.us"
        self.session_year = session_year

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

    def format_link(self, attachment_type, committee, link, chamber):
        if attachment_type == "witlistmtg":
            return (committee.split(" on ")[-1].strip() if " on " in committee else committee, link, chamber)
        return link

    def get_chamber_attachments(self, chamber, attachment_type):
        chamber_soup = BeautifulSoup(self.get_chamber_page(chamber), "html.parser")
        committees = chamber_soup.find_all("a", {"id": "CmteList"})
        links = []
        for committee in committees:
            com_soup = self.get_committee_page(committee["href"])
            all_links = com_soup.find_all("a", {"href": True, "target": True})
            links += [self.format_link(attachment_type,
                                       committee.text,
                                       self.create_attachment_link(link["href"]),
                                       chamber) for link in all_links if
                      link.img and attachment_type in link["href"] and "html" in link["href"]]
        return links

    def is_name(self, text):
        return "," in text and "(" in text and ")" in text and text.find(",") < text.find("(") and text.find(",") < text.find(")")

    def is_position(self, text):
        return text.lower() == "for:" or text.lower() == "against:" or text.lower() == "on:"

    def is_bill(self, text):
        return "HB " in text or "SB " in text

    def parse_date(self, link):
        parts = link.split("/")[-1][4:12]
        return str(parts[:4]) + "-" + str(parts[4:6]) + "-" + str(parts[6:])

    def append_line(self, line, url):
        # if self.broken_line and ("Del Valle" in line or "Del Valle" in self.broken_line) \
        #         and ("Nelson" in line or "Nelson" in self.broken_line):
        #     print("del valle and nelson")
        #     print(line)
        #     print(self.broken_line)
        #     print(url)
        #     exit()
        # if "Hartman" in line:
        #     print(line)
        #     print(line.strip()[-4:] == ", TX")
        #     print(line[:line.replace(", TX", "").rfind(",")] + ")")
        if "pronounced" in line:
            text_parts = line.replace(" (pronounced: ", ", ").split(" ")
            text_parts.pop(1)
            line = " ".join(text_parts)
            # print(text)
        # if line.count("(") > 1 and "))" not in line:
        #     line = line.replace("(", "\"", 1).replace(")", "\"", 1)
        if line.count("(") > line.count(")") and (line.endswith("Houston, Tx") or
                                                  line.endswith("Austin, Tx") or
                                                  line.endswith("San Antonio, Tx") or
                                                  line.endswith("Dallas, Tx")):
            line = line[:line.replace(", TX", "").rfind(",")] + ")"
        elif line.count("(") > line.count(")"):
            if line.endswith(")"):
                line = line + ")"
            else:
                self.broken_line = line
                self.counter += 1
                line = False
        elif (line.count("(") < line.count(")")) and self.broken_line:
            if not self.broken_line:
                print(line)
                print(url)
            appended = self.broken_line + " " + line
            if appended.count("(") == appended.count(")"):
                line = appended
                self.broken_line = None
                self.counter = 0
                # print("Fixed line: " + line)
            elif line.strip()[-4:] == ", TX":
                line = self.broken_line.strip().strip(",") + "), " + line
                self.broken_line = None
                self.counter = 0
                # print("Fixed line: " + line)
            else:
                line = False
                self.counter += 1
        return line

    def format_line_for_senate(self, text):
        if "(also " in text:
            text = text[:text.find("(also")] + text[text.find(")") + 1:]
            if ((text.count("(") == 0 and text.count(")") == 0) or (text.count("(") != text.count(")"))):
                self.broken_line = text
                self.counter += 1
                return False
        comma_loc = text.find(",")
        end_name_loc = comma_loc + 2 + text[comma_loc + 2:].find(" ")
        name = text[:end_name_loc].strip()
        org = text[text.find("("):text.find(")")].strip(",")
        return name + " " + org

    def parse_witness_list(self, committee_name, witness_list_url, chamber):
        witness_soup = BeautifulSoup(requests.get(witness_list_url).text, "html.parser")
        lines = witness_soup.find_all("span")
        hearing_date = self.parse_date(witness_list_url)
        position = None
        bill = None
        session = None
        witness_list = []
        for line in lines:
            if self.counter > 5:
                print(repr(line.text))
                print(repr(self.broken_line))
                print(witness_list_url)
                print("over counter")
                exit()
            # removes the pronunciation of the word.
            text = line.text.replace("\xa0", "").replace("\r\n", " ").strip()
            #print("starting: " + text)
            #if chamber == "S":
            text = self.append_line(text, witness_list_url)

            if text and self.is_name(text) and bill:
                #print("before: " + text)
                text = self.format_line_for_senate(text)
                #print("after: " + str(text))
                if text and position:
                    #print("text and pos")
                    text = text.replace("Self", "").replace(";", "").replace(")", "")
                    #print("No Self: " + text)

                    text_parts = text.split("(")
                    name_parts = text_parts[0].strip().split(",")
                    # if len(name_parts) < 2:
                    #     print("name")
                    #     print(text)
                    #     print(witness_list_url)
                    #     print(name_parts)
                    # if len(text_parts) < 2:
                    #     print("text")
                    #     print(text)
                    #     print(witness_list_url)
                    #     print(text_parts)
                    #name = name_parts[1].strip() + " " + name_parts[0].strip()
                    organization_name = text_parts[1].strip() if len(text_parts) > 1 and len(text_parts[1].strip()) > 0 else None
                    #print("Final: " + name + " " + organization_name if organization_name else "")
                    if position == None:
                        print(witness_list_url)
                    if bill == None:
                        print(witness_list_url)
                    #print("org name")
                    if organization_name and ";" in organization_name:
                        orgs = organization_name.split(";")
                        for org in orgs:
                            #print(org)
                            witness_list.append(Witness(first=name_parts[1].strip(),
                                                        last=name_parts[0].strip(),
                                                        position=position,
                                                        session_year=self.session_year,
                                                        bill=bill,
                                                        chamber=chamber,
                                                        session = session,
                                                        organization_name=org,
                                                        hearing_date=hearing_date,
                                                        committee_name=committee_name,
                                                        source=witness_list_url,
                                                        state="TX"))
                    else:
                        #print("wit")
                        witness_list.append(Witness(first=name_parts[1].strip(),
                                                    last = name_parts[0].strip(),
                                                    position=position,
                                                    session_year=self.session_year,
                                                    bill = bill,
                                                    chamber=chamber,
                                                    session = session,
                                                    organization_name=organization_name,
                                                    hearing_date=hearing_date,
                                                    committee_name=committee_name,
                                                    source=witness_list_url,
                                                    state="TX"))
            elif text and self.is_position(text):
                position = text.strip(":")

            elif text and self.is_bill(text) and bill != text:
                if line.a and line.a["href"]:
                    bill = text
                    session = "1" if line.a["href"].split("&")[0][-1] == "1" else "0"
                else:
                    bill = None
                # print("line")
                # print(line)
                # print("line.a")
                # print(line.a)
                # print("line a href")
                # print(line.a["href"])

                # print(line["href"])
        return witness_list




    def get_all_witnesses(self):
        all_witness = []
        print("getting house")
        witness_list_info = self.get_house_witness_list()
        print("got house")
        #witness_list_info = self.get_senate_witness_list()
        # print("got senate")
        # print(witness_list_info)
        for committee_name, witness_list_url, chamber in witness_list_info:

            print("\n\n")
            print(committee_name)
            all_witness += self.parse_witness_list(committee_name, witness_list_url, chamber)

            #return all_witness
        return all_witness
# hearingParser = TxHearingPageParser()
# all_witnesses = hearingParser.get_all_witnesses()