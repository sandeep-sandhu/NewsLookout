#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_base_plugin.py
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

# ###################################

# import standard python libraries:
import sys
from pathlib import Path
import os

# import getopt
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


def setupEnviron():
    from pathlib import Path
    import os
    import sys
    cwd = Path(os.getcwd())
    sys.path.append(cwd.parent)
    sys.path.append(os.path.join(cwd.parent, 'plugins'))
    sys.path.append(os.path.join(cwd.parent, 'plugins_contrib'))
    import scraper_utils
    from scraper_app import NewsLookout


class BasePluginTestCase:

    def setUp(self):
        """Call before every test case."""
        print("setup test")
        setupEnviron()
        # load config

    def tearDown(self):
        """Call after every test case."""
        print("close test")

    def testBasePluginClass(self):
        """Test case Base Plugin Class
        """
        setupEnviron()
        appClassInst = NewsLookout()
        print(appClassInst.__name__)
        assert 1 == 1, "Base Plugin class is not calculating values correctly"


if __name__ == "__main__":
    setupEnviron()

# end of file
