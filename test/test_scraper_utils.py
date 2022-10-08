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
from . import getAppFolders, getMockAppInstance, list_all_files, read_bz2html_file


# ###################################


def test_checkAndParseDate():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_getNextDaysDate():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_getPreviousDaysDate():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_instClassFromFile():
    # TODO: implement this
    pass


def test_getNetworkLocFromURL():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_checkIfURLIsValid():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_extractLinks():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_normalizeURL():
    # TODO: implement this
    pass

def test_calculateCRC32():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_decodeSecret():
    pass


def test_checkAndSanitizeConfigString():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_checkAndSanitizeConfigInt():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_spaceGapAfterDot():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils


def test_fixSentenceGaps():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils
    # TODO: Test the following scenarios of full stop between sentences -
    # in the morning.A total
    # IST).The stock
    # Rs 167.75.Earlier,
    # 17.\nThat


def test_sameURLWithoutQueryParams_true():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils

    url1 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=x"
    url2 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=a"
    # url3 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotas?ticker=x"
    print(f'URL 1 {url1} should be identified as the same as URL 2 {url2}')
    compareResult = scraper_utils.sameURLWithoutQueryParams(url1, url2)
    assert compareResult is True, '1. sameURLWithoutQueryParams() is not identifying same URLs correctly'


def test_sameURLWithoutQueryParams_false():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils

    url1 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=x"
    # url2 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=a"
    url3 = "https://economictimes.indiatimes.com/markets/stocks/stooo-qaaaas?ticker=x"
    print(f'URL 1 {url1} should not be identified as the same as URL 3 {url3}')
    compareResult = scraper_utils.sameURLWithoutQueryParams(url1, url3)
    assert compareResult is False, '2. sameURLWithoutQueryParams() is not identifying same URLs correctly'

def test_retainValidArticles():
    # check filtering out of valid urls based on valid string pattern
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils

    articleList = ['https://economictimes.indiatimes.com/news/latest-news/most-commented',
                   'https://www.twitter.com',
                   'https://auto.economictimes.indiatimes.com/xyz']
    validURLStringsToCheck = ['economictimes.indiatimes.com/']
    resultList = scraper_utils.retainValidArticles(articleList, validURLStringsToCheck)
    assert 'https://economictimes.indiatimes.com/news/latest-news/most-commented' in resultList,\
        '3. retainValidArticles() is excluding valid URLs!'
    assert 'https://auto.economictimes.indiatimes.com/xyz' in resultList, \
        '3. retainValidArticles() is excluding valid URLs!'
    assert 'https://www.twitter.com' not in resultList, \
        '3. retainValidArticles() is not filtering out invalid URLs!'

def test_removeInValidArticles():
    """ Test removal of invalid article URLs from url list
    """
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils

    articleList = ['https://economictimes.indiatimes.com/news/latest-news/most-commented',
                   'https://www.twitter.com/@ecotimes',
                   'https://www.facebook.com/abcd',
                   'https://auto.economictimes.indiatimes.com/xyz',
                   'https://indianexpress.com/section/entertainment/bollywood/box-office-collection/']
    invalidURLSubStrings = ['indianexpress.com/section/entertainment/',
                            '//www.indiatimes.com/',
                            '//timesofindia.indiatimes.com/',
                            '//economictimes.indiatimes.com/et-search/',
                            '//economictimes.indiatimes.com/hindi',
                            '/videoshow/',
                            '/slideshow/',
                            '/podcast/',
                            '/panache/',
                            'economictimes.indiatimes.com/terms-conditions',
                            'economictimes.indiatimes.com/privacypolicy.cms',
                            'economictimes.indiatimes.com/codeofconduct.cms',
                            'economictimes.indiatimes.com/plans.cms',
                            'https://economictimes.indiatimes.com/subscription',
                            '/slideshowlist/',
                            '/news/elections/',
                            'www.facebook.com/',
                            'economictimes.indiatimes.com/privacyacceptance.cms']
    resultList = scraper_utils.removeInValidArticles(articleList, invalidURLSubStrings)
    assert 'https://economictimes.indiatimes.com/news/latest-news/most-commented' in resultList, \
        '4. removeInValidArticles() is wrongly excluding valid URLs!'
    assert 'https://indianexpress.com/section/entertainment/bollywood/box-office-collection/' not in resultList, \
        '4. removeInValidArticles() is not filtering out invalid URLs!'
    assert 'https://www.facebook.com/abcd' not in resultList, \
        '4. removeInValidArticles() is not filtering out invalid URLs!'

def test_removeStartTrailQuotes():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils

    assert scraper_utils.removeStartTrailQuotes('"some text"') == 'some text',\
        "5. removeStartTrailQuotes() is not removing quotes around text correctly"
    assert scraper_utils.removeStartTrailQuotes('"another " text') == 'another " text', \
        "5. removeStartTrailQuotes() is not removing quotes around text correctly"

def test_saveObjToJSON():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils

    jsonFileName = 'test_saveObjToJSON.json'
    objToSave = {'key1': 1, 'key2':'second value'}
    scraper_utils.saveObjToJSON(jsonFileName, objToSave)
    import json
    with open(jsonFileName, 'r', encoding='utf-8') as fp:
        objToTest = json.load(fp)
        fp.close()
    # clean up afterwards
    os.remove(jsonFileName)
    assert 'key1' in objToTest, "6. saveObjToJSON() is not saving data structure to JSON file correctly"
    assert objToTest['key1']== objToSave['key1'],\
        "6. saveObjToJSON() is not saving data structure to JSON file correctly"
    assert objToTest['key2']== objToSave['key2'],\
        "6. saveObjToJSON() is not saving data structure to JSON file correctly"
    assert 'key2' in objToTest, "6. saveObjToJSON() is not saving data structure to JSON file correctly"


def test_deDupeList():
    # Test to deduplicate list
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils

    listWithDuplicates = ['one', 'two', 'two', 'three']
    resultList = scraper_utils.deDupeList(listWithDuplicates)
    print('Resultng list after de-duplicating:', resultList)
    assert len(resultList)==3 and 'two' in resultList, "8. deDupeList() is not de-duplicating lists correctly."


def test_filterRepeatedchars():
    # test to filter out Repeated charaters
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import scraper_utils

    baseText = 'A good sentence with repeated    spaces and tabs \t\t\t and\n\n\n newlines and hyphens---- dots....'
    charList = [' ', '\t', '\n', '-']
    resultText = scraper_utils.filterRepeatedchars(baseText, charList)
    print('Result after filtering repeated characters:\n', resultText)
    assert resultText == "A good sentence with repeated spaces and tabs \t and\n newlines and hyphens- dots....",\
        "10. filterRepeatedchars() is not filtering repeated characters correctly."


if __name__ == "__main__":
    test_sameURLWithoutQueryParams_true()

# end of file
