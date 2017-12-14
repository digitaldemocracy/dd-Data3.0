'''
This class is a wrapper around the open states api.
During unit testing, this class can be mocked and
the unit test can be handed fake data.
Author: Nick Russo
Date: 12/14/2017
'''
import requests

class OpenStatesAPI(object):
    def __init__(self, state):
        self.state = state
        self.OPENSTATES_API_KEY = "3017b0ca-3d4f-482b-9865-1c575283754a"
        self.LEGISLATORS_SEARCH_URL = 'https://openstates.org/api/v1/legislators/?state={0}&active=true&apikey=' + self.OPENSTATES_API_KEY
        self.LEGISLATORS_DETAIL_URL = 'https://openstates.org/api/v1/legislators/{0}&apikey=' + self.OPENSTATES_API_KEY
        self.COMMITTEE_SEARCH_URL = "https://openstates.org/api/v1/committees/?state={0}"
        self.COMMITTEE_SEARCH_URL += "&apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

        self.COMMITTEE_DETAIL_URL = "https://openstates.org/api/v1/committees/{0}/"
        self.COMMITTEE_DETAIL_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

        self.STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
        self.STATE_METADATA_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

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
