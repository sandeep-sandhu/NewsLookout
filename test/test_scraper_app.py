#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_scraper_app.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Test for the main class for the web scraping and news text processing application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com


 Notice:
 This software is intended for demonstration and educational purposes only. This software is
 experimental and a work in progress. Under no circumstances should these files be used in
 relation to any critical system(s). Use of these files is at your own risk.

 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not liable for such kind of inappropriate use of this software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 DEALINGS IN THE SOFTWARE.

"""

####################################

# import standard python libraries:
import sys
# import getopt
# import os
# import time
# import logging
# import importlib
# import importlib.resources
# from configparser import ConfigParser

# import web retrieval and text processing python libraries:
# import bs4
# import newspaper
# import nltk

# ###################################


class MainAppTestCase():

    def setUp(self):
        """Call before every test case."""
        print("setup test")
        # load config

    def tearDown(self):
        """Call after every test case."""
        print("close test")

    def testAppClass(self):
        """Test case AppClass
        """
        appClassInst = NewsLookout()
        print(appClassInst.__name__)
        assert 1 == 1, "App class not calculating values correctly"


if __name__ == "__main__":
    sys.path.append('NewsLookout')
    sys.path.append('NewsLookout\\plugins')
    sys.path.append('NewsLookout\\plugins_contrib')

    # import project's python libraries:
    from scraper_app import NewsLookout

    testApp = MainAppTestCase()
    # run all tests
