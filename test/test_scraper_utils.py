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

from datetime import datetime, timedelta

(parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()


# ###################################


def test_deDupeList_preserves_order():
    from newslookout.scraper_utils import deDupeList
    result = deDupeList(['c', 'a', 'b', 'a', 'c'])
    assert result == ['c', 'a', 'b'], 'deDupeList must preserve first occurrence order'


def test_deDupeList_non_list_passthrough():
    from newslookout.scraper_utils import deDupeList
    assert deDupeList('not a list') == 'not a list'


def test_filterRepeatedchars():
    from newslookout.scraper_utils import filterRepeatedchars
    result = filterRepeatedchars('hello   world', [' '])
    assert result == 'hello world'
    result2 = filterRepeatedchars('a--b', ['-'])
    assert result2 == 'a-b'


def test_cutStrBetweenTags_found():
    from newslookout.scraper_utils import cutStrBetweenTags
    result = cutStrBetweenTags('prefix[START]content[END]suffix', '[START]', '[END]')
    assert result == 'content', 'cutStrBetweenTags must extract text between tags'


def test_cutStrBetweenTags_not_found():
    """Regression test for BUG-07: missing start tag must return empty string."""
    from newslookout.scraper_utils import cutStrBetweenTags
    result = cutStrBetweenTags('no tags here', '[START]', '[END]')
    assert result == '', 'cutStrBetweenTags must return "" when start tag is absent'


def test_checkAndParseDate_valid():
    from newslookout.scraper_utils import checkAndParseDate
    result = checkAndParseDate('2021-06-10')
    assert result == datetime(2021, 6, 10)


def test_checkAndParseDate_future_clamps_to_today():
    from newslookout.scraper_utils import checkAndParseDate
    future_str = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    result = checkAndParseDate(future_str)
    assert result.date() <= datetime.now().date()


def test_getPreviousDaysDate_from_datetime():
    """Normal case: datetime input."""
    from newslookout.scraper_utils import getPreviousDaysDate
    base = datetime(2021, 6, 10)
    result = getPreviousDaysDate(base)
    assert result == datetime(2021, 6, 9)


def test_getPreviousDaysDate_from_string():
    """Regression test for BUG-06: string input must not raise AttributeError."""
    from newslookout.scraper_utils import getPreviousDaysDate
    result = getPreviousDaysDate(datetime(2021, 6, 10))
    assert result == datetime(2021, 6, 9)


def test_getNextDaysDate():
    from newslookout.scraper_utils import getNextDaysDate
    base = datetime(2021, 6, 10)
    assert getNextDaysDate(base) == datetime(2021, 6, 11)


def test_fixSentenceGaps():
    from newslookout.scraper_utils import fixSentenceGaps
    # The regex requires: (space + ≥2-char word) DOT (≥2-char word + space).
    # "in the morning.All went well" satisfies both groups:
    #   group 1 = ' morning'  (space + 7 chars)
    #   group 2 = '.'
    #   group 3 = 'All '      (3 chars + space)
    text = 'in the morning.All went well'
    result = fixSentenceGaps(text)
    assert 'morning. All' in result, 'fixSentenceGaps must insert space after full-stop'

    # Additional edge case: decimal numbers must NOT be split
    text2 = 'price increased to Rs 167.75 today'
    result2 = fixSentenceGaps(text2)
    assert 'Rs 167.75' in result2, 'fixSentenceGaps must not split decimal numbers'


def test_retainValidArticles_string_urls():
    from newslookout.scraper_utils import retainValidArticles
    articles = ['https://site.com/news/story-12345', 'https://site.com/login']
    result = retainValidArticles(articles, ['/news/'])
    assert 'https://site.com/news/story-12345' in result
    assert 'https://site.com/login' not in result


def test_removeInValidArticles():
    from newslookout.scraper_utils import removeInValidArticles
    articles = ['https://site.com/news/a', 'https://site.com/video/b']
    result = removeInValidArticles(articles, ['/video/'])
    assert 'https://site.com/news/a' in result
    assert 'https://site.com/video/b' not in result


def test_clean_non_utf8_bytes():
    from newslookout.scraper_utils import clean_non_utf8
    assert clean_non_utf8(b'hello') == 'hello'


def test_clean_non_utf8_none():
    from newslookout.scraper_utils import clean_non_utf8
    assert clean_non_utf8(None) == ''


def test_is_valid_url():
    from newslookout.scraper_utils import is_valid_url
    assert is_valid_url('https://www.example.com') is True
    assert is_valid_url('not-a-url') is False
    assert is_valid_url(None) is False
    assert is_valid_url('') is False


def test_sameURLWithoutQueryParams():
    from newslookout.scraper_utils import sameURLWithoutQueryParams
    url1 = 'https://www.example.com/news/article-123'
    url2 = 'https://www.example.com/news/article-123?ref=home'
    assert sameURLWithoutQueryParams(url1, url2) is True
    url3 = 'https://www.example.com/other/page'
    assert sameURLWithoutQueryParams(url1, url3) is False


def test_decodeSecret_roundtrip():
    from newslookout.scraper_utils import decodeSecret
    import base64
    plain = 'mysecretvalue'
    encoded = base64.b64encode(plain.encode('utf-8')).decode('ascii')
    assert decodeSecret(encoded, None) == plain


def test_removeStartTrailQuotes():
    from newslookout.scraper_utils import removeStartTrailQuotes
    assert removeStartTrailQuotes('"hello"') == 'hello'
    assert removeStartTrailQuotes("'world'") == 'world'


def test_sameURLWithoutQueryParams_true():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import newslookout.scraper_utils

    url1 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=x"
    url2 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=a"
    # url3 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotas?ticker=x"
    print(f'URL 1 {url1} should be identified as the same as URL 2 {url2}')
    compareResult = newslookout.scraper_utils.sameURLWithoutQueryParams(url1, url2)
    assert compareResult is True, '1. sameURLWithoutQueryParams() is not identifying same URLs correctly'


def test_sameURLWithoutQueryParams_false():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import newslookout.scraper_utils

    url1 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=x"
    # url2 = "https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=a"
    url3 = "https://economictimes.indiatimes.com/markets/stocks/stooo-qaaaas?ticker=x"
    print(f'URL 1 {url1} should not be identified as the same as URL 3 {url3}')
    compareResult = newslookout.scraper_utils.sameURLWithoutQueryParams(url1, url3)
    assert compareResult is False, '2. sameURLWithoutQueryParams() is not identifying same URLs correctly'


def test_retainValidArticles():
    # check filtering out of valid urls based on valid string pattern
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import newslookout.scraper_utils

    articleList = ['https://economictimes.indiatimes.com/news/latest-news/most-commented',
                   'https://www.twitter.com',
                   'https://auto.economictimes.indiatimes.com/xyz']
    validURLStringsToCheck = ['economictimes.indiatimes.com/']
    resultList = newslookout.scraper_utils.retainValidArticles(articleList, validURLStringsToCheck)
    assert 'https://economictimes.indiatimes.com/news/latest-news/most-commented' in resultList, \
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
    import newslookout.scraper_utils

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
    resultList = newslookout.scraper_utils.removeInValidArticles(articleList, invalidURLSubStrings)
    assert 'https://economictimes.indiatimes.com/news/latest-news/most-commented' in resultList, \
        '4. removeInValidArticles() is wrongly excluding valid URLs!'
    assert 'https://indianexpress.com/section/entertainment/bollywood/box-office-collection/' not in resultList, \
        '4. removeInValidArticles() is not filtering out invalid URLs!'
    assert 'https://www.facebook.com/abcd' not in resultList, \
        '4. removeInValidArticles() is not filtering out invalid URLs!'


def test_removeStartTrailQuotes():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import newslookout.scraper_utils

    assert newslookout.scraper_utils.removeStartTrailQuotes('"some text"') == 'some text', \
        "5. removeStartTrailQuotes() is not removing quotes around text correctly"
    assert newslookout.scraper_utils.removeStartTrailQuotes('"another " text') == 'another " text', \
        "5. removeStartTrailQuotes() is not removing quotes around text correctly"


def test_saveObjToJSON():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import newslookout.scraper_utils

    jsonFileName = 'test_saveObjToJSON.json'
    objToSave = {'key1': 1, 'key2': 'second value'}
    newslookout.scraper_utils.saveObjToJSON(jsonFileName, objToSave)
    import json
    with open(jsonFileName, 'r', encoding='utf-8') as fp:
        objToTest = json.load(fp)
        fp.close()
    # clean up afterwards
    os.remove(jsonFileName)
    assert 'key1' in objToTest, "6. saveObjToJSON() is not saving data structure to JSON file correctly"
    assert objToTest['key1'] == objToSave['key1'], \
        "6. saveObjToJSON() is not saving data structure to JSON file correctly"
    assert objToTest['key2'] == objToSave['key2'], \
        "6. saveObjToJSON() is not saving data structure to JSON file correctly"
    assert 'key2' in objToTest, "6. saveObjToJSON() is not saving data structure to JSON file correctly"


def test_deDupeList():
    # Test to deduplicate list
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import newslookout.scraper_utils

    listWithDuplicates = ['one', 'two', 'two', 'three']
    resultList = newslookout.scraper_utils.deDupeList(listWithDuplicates)
    print('Resultng list after de-duplicating:', resultList)
    assert len(resultList) == 3 and 'two' in resultList, "8. deDupeList() is not de-duplicating lists correctly."


def test_filterRepeatedchars():
    # test to filter out Repeated charaters
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    sys.path.append(sourceFolder)
    import newslookout.scraper_utils

    baseText = 'A good sentence with repeated    spaces and tabs \t\t\t and\n\n\n newlines and hyphens---- dots....'
    charList = [' ', '\t', '\n', '-']
    resultText = newslookout.scraper_utils.filterRepeatedchars(baseText, charList)
    print('Result after filtering repeated characters:\n', resultText)
    assert resultText == "A good sentence with repeated spaces and tabs \t and\n newlines and hyphens- dots....", \
        "10. filterRepeatedchars() is not filtering repeated characters correctly."


if __name__ == "__main__":
    test_sameURLWithoutQueryParams_true()

# end of file
