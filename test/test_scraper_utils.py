#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_scraper_app.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
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
import os


# ###################################


def test_sameURLWithoutQueryParams_true():
    # parentFolder = '..\\'
    parentFolder = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    sourceFolder = os.path.join(parentFolder, 'src')
    sys.path.append(sourceFolder)

    import scraper_utils
    url1 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=x"
    url2 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=a"
    # url3 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotas?ticker=x"
    compareResult = scraper_utils.sameURLWithoutQueryParams(url1, url2)
    assert compareResult is True


def test_sameURLWithoutQueryParams_false():
    # parentFolder = '..\\'
    parentFolder = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    sys.path.append(parentFolder)
    import scraper_utils
    url1 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=x"
    # url2 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=a"
    url3 = "https://economictimes.indiatimes.com/markets/stocks/stooo-qaaaas?ticker=x"
    compareResult = scraper_utils.sameURLWithoutQueryParams(url1, url3)
    assert compareResult is False


if __name__ == "__main__":
    test_sameURLWithoutQueryParams_true()

# end of file
