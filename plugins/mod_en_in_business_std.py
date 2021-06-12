#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_business_std.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Business Standard Newspaper
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

##########

# import standard python libraries:
import logging
import re

# import web retrieval and text processing python libraries:
# from bs4 import BeautifulSoup

# import this project's python libraries:
from base_plugin import basePlugin
# from scraper_utils import getNetworkLocFromURL
from data_structs import Types
from scraper_utils import deDupeList, filterRepeatedchars

##########

logger = logging.getLogger(__name__)


class mod_en_in_business_std(basePlugin):
    """ Web Scraping plugin - mod_en_in_business_std
    Description: Business Standard
    Language: English
    Country: India
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = "https://www.business-standard.com/"

    mainURLDateFormatted = "https://www.business-standard.com/advance-search?type=print-media&print_date=%d-%m-%Y"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ['https://www.business-standard.com/rss/home_page_top_stories.rss',
                     'https://www.business-standard.com/rss/latest.rss',
                     'https://www.business-standard.com/rss/markets-106.rss',
                     'https://www.business-standard.com/rss/economy-policy-102.rss',
                     'https://www.business-standard.com/rss/finance-103.rss']

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['www.business-standard.com/article/']

    nonContentStrings = ['/article/opinion/']

    nonContentURLs = all_rss_feeds + [
        mainURL,
        'https://books.business-standard.com',
        'https://bsmedia.business-standard.com',
        'https://epaper.business-standard.com',
        'https://hindi.business-standard.com',
        'https://s.business-standard.com',
        'https://smartinvestor.business-standard.com',
        'https://www.business-standard.com/android',
        'https://www.business-standard.com/author',
        'https://www.business-standard.com/b2b-connect',
        'https://www.business-standard.com/budget',
        'https://www.business-standard.com/companies',
        'https://www.business-standard.com/cookie-policy',
        'https://www.business-standard.com/coronavirus',
        'https://www.business-standard.com/disclaimer',
        'https://www.business-standard.com/education',
        'https://www.business-standard.com/finance',
        'https://www.business-standard.com/general-news',
        'https://www.business-standard.com/international',
        'https://www.business-standard.com/ipad',
        'https://www.business-standard.com/iphone',
        'https://www.business-standard.com/latest-news',
        'https://www.business-standard.com/management',
        'https://www.business-standard.com/markets',
        'https://www.business-standard.com/markets-ipos',
        'https://www.business-standard.com/markets-news',
        'https://www.business-standard.com/multimedia',
        'https://www.business-standard.com/opinion',
        'https://www.business-standard.com/pf',
        'https://www.business-standard.com/pf-features',
        'https://www.business-standard.com/pf-news',
        'https://www.business-standard.com/pf-news-loans',
        'https://www.business-standard.com/pf-news-tax',
        'https://www.business-standard.com/podcast',
        'https://www.business-standard.com/politics',
        'https://www.business-standard.com/poll',
        'https://www.business-standard.com/portfolio',
        'https://www.business-standard.com/specials',
        'https://www.business-standard.com/sports',
        'https://www.business-standard.com/technology',
        'https://www.business-standard.com/todays-paper',
        'https://www.business-standard.com/wap'
        ]

    # never fetch URLs containing these strings:
    invalidURLSubStrings = ['hindi.business-standard.com', '/sports']

    urlUniqueRegexps = [r'(^http.+\/\/)(www.business\-standard.com\/.+\-)([0-9]{5,})',
                        r'(^http.+\/\/)(www.business\-standard.com\/article.+\-)([0-9]{5,})(_1.html)',
                        r'(^http.+\/\/)(www.business\-standard.com\/article.+article_id=)([0-9]{5,})(_*[0-9]*)']

    articleDateRegexps = {
        # "datePublished": "2021-02-25T22:59:00+05:30"
        r"(\"datePublished\": \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")":
        "%Y-%m-%dT%H:%M:%S",
        # content = "Fri, 26 Feb 2021 02:33:38 +0530">
        r"(content = \")([a-zA-Z]{3}, [0-9]{1,2} [a-zA-Z]{3} 20[0-9]{2} [0-9]{1,2}:[0-9]{2}:[0-9]{2} \+0530)(\">)":
        "%a, %d %b %Y %H:%M:%S %z",
        r'(<strong>)([a-zA-Z]{3} [0-9]{1,2}, 20[0-9]{2})(<\/strong>)': '%b %d, %Y'
        }

    invalidTextStrings = ['Support quality journalism and subscribe to Business Standard',
                          'Business Standard has always strived hard to provide up-to-date information'
                          ]
    subStringsToFilter = ['(Only the headline and picture of this report may have been reworked by the Business Standard' +
                          ' staff; the rest of the content is auto-generated from a syndicated feed.)']
    allowedDomains = ['www.business-standard.com']

    articleIndustryRegexps = []

    authorRegexps = []

    authorMatchPatterns = []
    urlMatchPatterns = []
    dateMatchPatterns = dict()
    listOfURLS = []

    def __init__(self):
        # use base class's lists and dicts in searching for unique url and published date strings
        self.articleDateRegexps.update(basePlugin.articleDateRegexps)
        self.urlUniqueRegexps = self.urlUniqueRegexps + super().urlUniqueRegexps
        super().__init__()

    def extractIndustries(self, uRLtoFetch, htmlText):
        """ Extract the industry of the articles from its URL or contents
        """
        industries = []
        if type(htmlText) == bytes:
            htmlText = htmlText.decode('UTF-8')
        return(industries)

    def extractAuthors(self, htmlText):
        """ extract the author from the html content
        """
        authors = []
        if type(htmlText) == bytes:
            htmlText = htmlText.decode('UTF-8')
        authorPattern = re.compile(r'(<meta name="author" content=")([a-zA-Z0-9 _\-]+)(">)')
        matchRes = authorPattern.search(htmlText)
        if matchRes is not None:
            authors.append(matchRes.group(2))
        return(authors)

    # https://www.business-standard.com/article/economy-policy/sporadic-lockdowns-to-cost-india-1-25-billion-per-week-barclays-121041200631_1.html
    def extractArticleBody(self, htmlContent):
        """ extract the text body of the article
        """
        body_text = ""
        if type(htmlContent) == bytes:
            htmlContent = htmlContent.decode('UTF-8')
        return(body_text)

    def checkAndCleanText(self, inputText, rawData):
        """ Check and clean article text
        """
        cleanedText = inputText
        invalidFlag = False
        try:
            for badString in self.invalidTextStrings:
                if cleanedText.find(badString) >= 0:
                    logger.debug("%s: Found invalid text strings in data extracted: %s", self.pluginName, badString)
                    invalidFlag = True
            # check if article content is not valid or is too little
            if invalidFlag is True or len(cleanedText) < self.minArticleLengthInChars:
                cleanedText = self.extractArticleBody(rawData)
            # replace repeated spaces, tabs, hyphens, '\n', '\r\n', etc.
            cleanedText = filterRepeatedchars(cleanedText,
                                              deDupeList([' ', '\t', '\n', '\r\n', '-', '_', '.']))
            # remove invalid substrings:
            for stringToFilter in deDupeList(self.subStringsToFilter):
                cleanedText = cleanedText.replace(stringToFilter, " ")
        except Exception as e:
            logger.error("Error cleaning text: %s", e)
        return(cleanedText)


# # end of file ##
