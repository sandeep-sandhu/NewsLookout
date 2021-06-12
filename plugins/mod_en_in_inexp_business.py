#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_inexp_business.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for the Indian Express - Business news portal
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

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup

# import this project's python libraries:
from base_plugin import basePlugin
from scraper_utils import deDupeList, filterRepeatedchars
from data_structs import Types

##########

logger = logging.getLogger(__name__)

##########


class mod_en_in_inexp_business(basePlugin):
    """ Web Scraping plugin - mod_en_in_inexp_business for Indian Express Business Newspaper
    """

    minArticleLengthInChars = 250

    pluginType = Types.MODULE_NEWS_CONTENT  # implies web-scraper for news content

    mainURL = 'https://www.newindianexpress.com/business/'

    all_rss_feeds = ["https://www.newindianexpress.com/Nation/rssfeed/?id=170&getXmlFeed=true"]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['https://www.newindianexpress.com/nation/',
                              'business',
                              'https://www.newindianexpress.com/opinions/',
                              'https://www.newindianexpress.com/world/',
                              'https://indianexpress.com/']

    nonContentStrings = ['https://www.newindianexpress.com/opinions/']

    # get URL links from these URLs but done fetch content from them:
    nonContentURLs = ['https://www.newindianexpress.com/opinions/editorials',
                      'https://www.newindianexpress.com/opinions/columns',
                      'https://www.newindianexpress.com/opinions/columns/karamatullah-k-ghori',
                      'https://www.newindianexpress.com/opinions/columns/shampa-dhar-kamath',
                      'https://www.newindianexpress.com/opinions/columns/shankkar-aiyar',
                      'https://www.newindianexpress.com/opinions/columns/ravi-shankar',
                      'https://www.newindianexpress.com/opinions/columns/s-gurumurthy',
                      'https://www.newindianexpress.com/opinions/columns/t-j-s-george']

    # never fetch these URLs:
    invalidURLSubStrings = []

    urlUniqueRegexps = [r"(^https.*)(\-)([0-9]+)(\.html$)",
                        r"(^https\://indianexpress.com/article/.*)(\-)([0-9]+)(/$)",
                        r"(^https\://indianexpress.com/article/.*)(\-)([0-9]+)(\.html$)"
                        ]
    urlMatchPatterns = []

    invalidTextStrings = []
    subStringsToFilter = []
    articleDateRegexps = {r'("datePublished":")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})' +
                          r'(\+05:30","dateModified")':
                          '%Y-%m-%dT%H:%M:%S',
                          r'(Published: <span>)([0-9]{1,}th[ ]+[A-Za-z]{3,} 20[0-9]{2} [0-9]{2}:[0-9]{2})( .M<\/span>)':
                          '%dth  %B %Y %H:%M'
                          }

    dateMatchPatterns = dict()

    authorRegexps = [r"(\"author\":{\"\@type\":\"Person\",\"name\":\")([a-zA-Z_\-\. ]{2,})(\"\})",
                     r"(<span class=\"author_des\"> By <span>)([a-zA-Z_\-\. ]{2,})(<\/span>)"
                     ]
    authorMatchPatterns = []

    allowedDomains = ["indianexpress.com", "www.newindianexpress.com"]
    listOfURLS = []
    uRLdata = dict()
    urlMatchPatterns = []

    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(basePlugin.articleDateRegexps)
        self.urlUniqueRegexps = super().urlUniqueRegexps + self.urlUniqueRegexps
        super().__init__()

    def extractIndustries(self, uRLtoFetch, htmlText):
        """  Extract Industries relevant to the article from URL or html content
        """
        industries = []
        try:
            logger.debug("Extracting industries identified by the article.")
            # docRoot = BeautifulSoup(htmlText, 'lxml')
            # section = article_html.find( "span", "ag")
        except Exception as e:
            logger.error("When extracting industries: %s", e)
        return(industries)

    def extractAuthors(self, htmlText):
        """ Extract Authors/Agency/Source from html
        """
        authors = []
        maxAuthorStringLength = 100
        authorStr = None
        for authorMatch in self.authorMatchPatterns:
            logger.debug("Trying match pattern: %s", authorMatch)
            try:
                result = authorMatch.search(htmlText)
                if result is not None:
                    authorStr = result.group(2)
                    # At this point, the text was correctly extracted, so exit the loop
                    break
            except Exception as e:
                logger.debug("Unable to identify the article authors using regex: %s; string to parse: %s, URL: %s",
                             e, authorStr, self.URLToFetch)
            if len(authorStr) < 1:
                raise Exception("Could not identify news agency/source.")
            elif len(authorStr) > maxAuthorStringLength:
                raise Exception("Could not identify news agency/source.")
            else:
                authors = authorStr.split(',')
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
            # except Warning as w:
            #    logger.warn("Warning when extracting text via BeautifulSoup: %s", w)
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
