#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_hindu.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for the Hindu newspaper
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
from bs4 import BeautifulSoup

from data_structs import Types
# from data_structs import ScrapeError
from base_plugin import basePlugin
from scraper_utils import deDupeList, filterRepeatedchars

##########

logger = logging.getLogger(__name__)


class mod_en_in_hindu(basePlugin):
    """ Web Scraping plugin - mod_en_in_hindu for the Hindu business Newspaper
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 200

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # archive URL: https://www.thehindu.com/archive/
    mainURL = 'https://www.thehindu.com/business/'
    mainURLDateFormatted = 'https://www.thehindu.com/archive/print/%Y/%m/%d/'

    all_rss_feeds = ["https://www.thehindu.com/business/feeder/default.rss"]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['https://www.thehindu.com/business/']

    # never fetch these URLs:
    invalidURLSubStrings = []

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://www.thehindu.com/business/agri-business/',
                      'https://www.thehindu.com/business/Industry/',
                      'https://www.thehindu.com/business/Economy/',
                      'https://www.thehindu.com/business/markets/',
                      'https://www.thehindu.com/business/budget/',
                      'https://www.thehindu.com/business/',
                      'https://epaper.thehindu.com',
                      'https://roofandfloor.thehindu.com',
                      'https://crossword.thehindu.com',
                      'https://frontline.thehindu.com',
                      'https://www.thehindu.com',
                      'https://step.thehindu.com',
                      'https://sportstar.thehindu.com'
                      ]

    nonContentStrings = ['epaper.thehindu.com',
                         'roofandfloor.thehindu.com',
                         'crossword.thehindu.com',
                         'frontline.thehindu.com',
                         'step.thehindu.com',
                         'sportstar.thehindu.com']

    urlUniqueRegexps = [r"(https\:\/\/)(www.thehindu.com\/business\/.*\-)([0-9]+)(\.ece$)",
                        r"(https\:\/\/www.thehindu.com\/business\/.*)(\-)([0-9]+)(/$)",
                        r"(https\:\/\/www.thehindu.com\/business\/.*)(article)([0-9]+)(\.ece)",
                        r"(https:\/\/)(www.thehindu.com\/news\/.+\/article)([0-9]{3,})(\.ece)"]

    invalidTextStrings = []
    subStringsToFilter = []
    articleDateRegexps = {
        r"(<meta name=\"publish-date\" content=\")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")":
        "%Y-%m-%dT%H:%M:%S",
        # January 22, 2015 15:30 IST
        r"(<none>\n)([a-zA-Z]{3,} [0-9]{1,2}, 20[0-9]{2} [0-9]{1,2}:[0-9]{2})( IST)":
        "%B %d, %Y %H:%M"}
    authorRegexps = []
    dateMatchPatterns = dict()
    urlMatchPatterns = []
    authorMatchPatterns = []

    allowedDomains = ["www.thehindu.com"]
    listOfURLS = []
    uRLdata = dict()
    urlMatchPatterns = []

    def __init__(self):
        """ Initialize the object
        Re-use base class's objects in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(super().articleDateRegexps)
        self.urlUniqueRegexps = self.urlUniqueRegexps + super().urlUniqueRegexps
        super().__init__()

    def extractIndustries(self, uRLtoFetch, htmlText):
        """ Extract the industry of the articles from its URL or contents
        """
        industries = []
        # <meta name="keywords" content="Go Air" />
        try:
            if type(htmlText) == bytes:
                htmlText = htmlText.decode('UTF-8')
            industryPat = re.compile(r"(<meta name=\"keywords\" content=\")([a-zA-Z_\-.\ ]{3,})(\" \/>)")
            matchRes = industryPat.search(htmlText)
            if matchRes is not None:
                industries.append(matchRes.group(2))
        except Exception as e:
            logger.error("Error extracting industries: %s", e)
        return(industries)

    def extractAuthors(self, htmlText):
        """ extract Authors/Agency/Source from html
        """
        authors = []
        try:
            if type(htmlText) == bytes:
                htmlText = htmlText.decode('UTF-8')
            authorPat = re.compile(r"(<meta property=\"article:author\" content=\")([a-zA-Z_\-.\ ]{3,})(\" \/>)")
            matchRes = authorPat.search(htmlText)
            if matchRes is not None:
                authors.append(matchRes.group(2))
        except Exception as e:
            logger.error("Error identifying authors of article: %s", e)
        return(authors)

    def extractArticleBody(self, htmlContent):
        """ Extract article's text from raw HTML content
         """
        articleText = ""
        try:
            # get article text data by parsing specific tags:
            docRoot = BeautifulSoup(htmlContent, 'lxml')
            matchParas = docRoot.findAll('p', {"class": 'body'})
            for para in matchParas:
                articleText = articleText + para.get_text()
            body_root = docRoot.find_all("div", "articlestorycontent")
            if len(body_root) > 0:
                articleText = body_root[0].getText()
        except Warning as w:
            logger.warning("Warning when extracting text via BeautifulSoup: %s", w)
        except Exception as e:
            logger.error("Error extracting article text via tags: %s", e)
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
