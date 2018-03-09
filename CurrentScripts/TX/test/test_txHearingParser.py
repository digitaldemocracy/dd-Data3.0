#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

from unittest import TestCase
from TX.tx_hearing_parser import TxHearingParser
from Models.Lobbyist import Lobbyist
from Utils.Generic_Utils import clean_name


class TestTxHearingParser(TestCase):
    def setUp(self):
        self.parser = TxHearingParser()
