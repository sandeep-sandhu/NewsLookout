#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_scraper_app.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Test for the main class for the web scraping and news text processing application
"""

__version__ = "1.6"
__author__ = "Sandeep Singh Sandhu"
__copyright__ = "Copyright 2020, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu"
__credits__ = ["Sandeep Singh Sandhu"]
__license__ = "GPL"
__maintainer__ = "Sandeep Singh Sandhu"
__email__ = "sandeep.sandhu@gmx.com"
__status__ = "Production"

####################################


import unittest


# import standard python libraries:
import sys, getopt, os
import time
import logging
import importlib
import importlib.resources
from configparser import ConfigParser 


# import web retrieval and text processing python libraries:
import bs4
import newspaper
import nltk
import spacy

# import project's python libraries:
from scraper_utils import checkAndParseDate, checkAndGetNLTKData, loadAndSetCookies
from queueManager import queueManager


####################################


class SimpleTestCase(unittest.TestCase):

    prefix = "C:\\scraper_py"
    install_path = '..\\bin'
    modules_path = '..\\plugins'
    configFileName = '..\\conf\\scraper.conf'

    
    def setUp(self):
        """Call before every test case."""
        print("setup unittest")
        # load config


    def tearDown(self):
        """Call after every test case."""
        print("close unittest")


    def testAppClass(self):
        """ Test case A. note that all test method names must begin with 'test.'"""
        sys.path.append( self.install_path )
        sys.path.append( self.modules_path )
        from scraper_app import scraper
        appClassInst = scraper()
        
        #assert 192 == 543, "192 == 543 not calculating values correctly"


    def testSplit(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)



if __name__ == "__main__":
    unittest.main() # run all tests

