'''
This class is a wrapper around the open states api.
During unit testing, this class can be mocked and
the unit test can be handed fake data.
Author: Nick Russo
Date: 12/14/2017
'''
import requests
import datetime as dt
class OpenStatesAPI(object):
    def __init__(self, state):
        self.state = state
        OPENSTATES_API_KEY = "3017b0ca-3d4f-482b-9865-1c575283754a"
        self.LEGISLATORS_SEARCH_URL = 'https://openstates.org/api/v1/legislators/?state={0}&active=true&apikey=' + OPENSTATES_API_KEY
        self.LEGISLATORS_DETAIL_URL = 'https://openstates.org/api/v1/legislators/{0}&apikey=' + OPENSTATES_API_KEY

        self.COMMITTEE_SEARCH_URL = "https://openstates.org/api/v1/committees/?state={0}"
        self.COMMITTEE_SEARCH_URL += "&apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

        self.COMMITTEE_DETAIL_URL = "https://openstates.org/api/v1/committees/{0}/"
        self.COMMITTEE_DETAIL_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

        self.STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
        self.STATE_METADATA_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

        self.BILL_SEARCH_URL = "https://openstates.org/api/v1/bills/?state={0}&search_window=session&updated_since={1}"
        self.BILL_SEARCH_URL += "&apikey=" + OPENSTATES_API_KEY

        self.BILL_DETAIL_URL = "https://openstates.org/api/v1/bills/{0}/"
        self.BILL_DETAIL_URL += "?apikey=" + OPENSTATES_API_KEY

        self.record_api_url = "http://api.followthemoney.org/?c-t-eid={0}&gro=c-t-id,d-id&p={1}&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json"
        # Get all candidates for a certain election year
        self.candidates_api_url = 'https://api.followthemoney.org/?s={0}&y={1}&c-exi=1&c-t-sts=1,9&c-r-ot=S,H&gro=c-t-id&p={2}&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json'
        self.updated_api_url = 'https://api.followthemoney.org/?s={0}&d-ludte={1},{2}&c-exi=1&c-t-sts=1,9&c-r-ot=S,H&gro=d-id,c-t-id&p={3}&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json'

    def get_legislators_json(self):
        '''
        Calls the openstates api for legislator json data.
        :return: a json list of openstates data.
        '''
        api_url = self.LEGISLATORS_SEARCH_URL.format(self.state.lower())
        return requests.get(api_url).json()

    def get_committee_membership_json(self, comm_alt_id):
        '''
        Calls the openstates api for committee information
        :return:
        '''
        api_url = self.COMMITTEE_DETAIL_URL.format(comm_alt_id)
        return requests.get(api_url).json()

    def get_committee_json(self):
        api_url = self.COMMITTEE_SEARCH_URL.format(self.state.lower())
        return requests.get(api_url).json()

    def get_state_metadate_json(self):
        metadata_url = self.STATE_METADATA_URL.format(self.state.lower())
        return requests.get(metadata_url).json()

    def get_bill_json(self):
        updated_date = dt.date.today() - dt.timedelta(weeks=1)
        updated_date = updated_date.strftime('%Y-%m-%d')
        api_url = self.BILL_SEARCH_URL.format(self.state.lower(), updated_date)
        return requests.get(api_url).json()


    def get_bill_detail(self, os_bid):
        api_url = self.BILL_DETAIL_URL.format(os_bid)
        return requests.get(api_url).json()

    def get_records_and_max_pages_json(self, eid, index):
        page = requests.get(self.record_api_url.format(eid, index))
        result = page.json()
        max_pages = result['metaInfo']['paging']['maxPage']
        return result['records'], max_pages

    def get_updated_records_and_max_pages_json(self, min_date, max_date, index):
        page = requests.get(self.updated_api_url.format(self.state, min_date, max_date, index))
        result = page.json()
        max_pages = result['metaInfo']['paging']['maxPage']
        return result['records'], max_pages

    def get_candidates_records_and_max_pages_json(self, year, index):
        url = self.candidates_api_url.format(self.state, year, index)
        page = requests.get(url)
        result = page.json()
        max_pages = result['metaInfo']['paging']['maxPage']
        return result['records'], max_pages