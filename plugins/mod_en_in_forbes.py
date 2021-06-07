#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_forbes.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for the Forbes India news portal


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

# #########

# import standard python libraries:
import logging
# import re

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup

from data_structs import Types
from scraper_utils import cutStrBetweenTags, filterRepeatedchars, deDupeList
# from data_structs import ScrapeError
from base_plugin import basePlugin

# #########

logger = logging.getLogger(__name__)


class mod_en_in_forbes(basePlugin):
    """ Web Scraping plugin: mod_en_in_forbesindia
    For Forbes india
    """
    minArticleLengthInChars = 400

    pluginType = Types.MODULE_NEWS_CONTENT  # implies web-scraper for news content

    mainURL = 'https://www.forbesindia.com/'
    all_rss_feeds = ["https://www.forbesindia.com/rssfeeds/rss_all.xml"]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['https://www.forbesindia.com/'
                              ]

    # get URL links from these URLs but don't fetch content from them:
    nonContentURLs = [mainURL,
                      'https://www.forbesindia.com/blog/',
                      'https://www.forbesindia.com/magazine/',
                      'https://www.forbesindia.com/features/enterprise/',
                      'https://www.forbesindia.com/subscription/index.php',
                      'https://www.forbesindia.com/multimedia/twinterview/',
                      'https://www.forbesindia.com/blog/category/technology/',
                      'https://www.forbesindia.com/multimedia/',
                      'https://www.forbesindia.com/blog/category/life/',
                      'https://www.forbesindia.com/blog/category/health/',
                      'https://www.forbesindia.com/forbesindiamagazine/',
                      'https://www.forbesindia.com/contactus/',
                      'https://www.forbesindia.com/advertise/',
                      'https://www.forbesindia.com/marquee',
                      'https://www.forbesindia.com/leadershipawards',
                      'https://www.forbesindia.com/privacy/',
                      'https://www.forbesindia.com/cookiepolicy/',
                      'https://www.forbesindia.com/terms-conditions/',
                      'https://www.forbesindia.com/disclaimer/',
                      'https://www.forbesindia.com/subscription/',
                      'https://www.forbesindia.com/features/corporate/',
                      'https://www.forbesindia.com/features/firstprinciples/',
                      'https://www.forbesindia.com/features/globalgame/',
                      'https://www.forbesindia.com/features/specialreport/',
                      'https://www.forbesindia.com/broadcast1/business-evangelist-of-india/',
                      'https://www.forbesindia.com/skilltree/education-evangelists-of-india/',
                      'https://www.forbesindia.com/aperture/',
                      'https://www.forbesindia.com/lists/1',
                      'https://www.forbesindia.com/rss/'
                      ]

    # get URL links from URLs containing these strings, but don't fetch content from them:
    nonContentStrings = ['www.forbesindia.com/multimedia/video/',
                         'www.forbesindia.com/video/',
                         'www.forbesindia.com/article/photo-of-the-day/',
                         'www.forbesindia.com/life/style/',
                         'www.forbesindia.com/life/tipoff/',
                         'www.forbesindia.com/life/frequent-flier/',
                         'www.forbesindia.com/life/nuggets/',
                         'www.forbesindia.com/search.php?'
                         ]

    # never fetch these URLs:
    invalidURLSubStrings = []

    urlUniqueRegexps = [r"(https\:\/\/www\.forbesindia\.com\/.+)(\/)([0-9]{4,})(/[0-9]+)",
                        r"(https\:\/\/www\.forbesindia\.com\/.+)(\/)([0-9]{4,})",
                        r"(https\:\/\/www\.forbesindia\.com/article/.*)(\-)([0-9]+)(\.html)"
                        ]

    invalidTextStrings = []
    subStringsToFilter = []
    articleDateRegexps = {}
    authorRegexps = []
    dateMatchPatterns = dict()
    urlMatchPatterns = []
    authorMatchPatterns = []

    allowedDomains = ["www.forbesindia.com"]
    listOfURLS = []
    uRLdata = dict()
    urlMatchPatterns = []

    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(basePlugin.articleDateRegexps)
        self.urlUniqueRegexps = self.urlUniqueRegexps + super().urlUniqueRegexps
        super().__init__()

    def extractIndustries(self, htmlText):
        """  extract Industries """
        industries = []
        try:
            logger.debug("Extracting industries identified by the article.")
            docRoot = BeautifulSoup(htmlText, 'lxml')
            docRoot.find("span", "ag")
        except Exception as e:
            logger.error("Error identifying the industries: %s", e)
        return(industries)

    def extractAuthors(self, htmlText):
        """ extract Authors/Agency/Source from html"""
        authors = []
        try:
            strNewsAgent = cutStrBetweenTags(htmlText, '<span class = "author_des">By', '</span></span>')
            strNewsAgent = cutStrBetweenTags(strNewsAgent, 'target = "_blank">', '</a>')
            if len(strNewsAgent) < 1:
                raise Exception("Could not identify news agency/source.")
            else:
                authors = [strNewsAgent]
        except Exception as e:
            logger.error("Error extracting news agent from text: %s", e)
        return(authors)

    def extractArticleBody(self, htmlContent):
        """ Extract article's text using the Beautiful Soup library """
        articleText = ""
        try:
            # get article text data by parsing specific tags:
            article_html = BeautifulSoup(htmlContent, 'lxml')
            # <div id = "storyContent" class = "articlestorycontent">
            body_root = article_html.find_all("div", "articlestorycontent")
            if len(body_root) > 0:
                articleText = body_root[0].getText()
        except Exception as e:
            logger.error("Exception extracting article via tags: %s", e)
        return(articleText)

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
