#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_trak.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Template to aid writing a custom plugin for the application
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
from data_structs import Types, ScrapeError
from scraper_utils import calculateCRC32, deDupeList, filterRepeatedchars
from base_plugin import basePlugin

##########

logger = logging.getLogger(__name__)


class mod_en_in_trak(basePlugin):
    """ Web Scraping plugin - mod_en_in_trak for Trak news portal.
    Language: English
    Country: India
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = "https://trak.in/"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ['https://trak.in/feed/rss/']

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['https://trak.in/']

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://trak.in/Tags/Business/category/internet/',
                      'https://trak.in/Tags/Business/category/internet/technology/',
                      'https://trak.in/Tags/Business/category/internet/ecommerce/',
                      'https://trak.in/Tags/Business/category/internet/gaming/',
                      'https://trak.in/Tags/Business/iot/',
                      'https://trak.in/Tags/Business/category/startup/',
                      'https://trak.in/Tags/Business/category/random/',
                      'https://trak.in/Tags/Business/category/telecom/',
                      'https://trak.in/Tags/Business/category/telecom/mobile/',
                      'https://trak.in/Tags/Business/category/india-business-opportunities-services-making-money/',
                      'https://trak.in/about-trakin-india-business-buzz-about-me/contact-me/',
                      'https://trak.in/about-trakin-india-business-buzz-about-me/',
                      'https://trak.in/tags/business/author/guest/',
                      'https://trak.in/Tags/Business/contactless-technology/',
                      'https://trak.in/feed/rss/',
                      'https://trak.in/tags/business/author/vishal-aaditya-kundu/',
                      'https://trak.in/tags/business/author/shreya-bose/',
                      'https://trak.in/tags/business/author/sheetal-bhalerao/',
                      'https://trak.in/tags/business/author/rohit-kulkarni/',
                      'https://trak.in/tags/business/author/radhika-kajarekar/',
                      'https://trak.in/tags/business/author/guest/',
                      'https://trak.in/tags/business/author/arpita-goria/',
                      'https://trak.in/Tags/Business/coronavirus/',
                      'https://trak.in/Tags/Business/category/telecom/gadget-2/',
                      'https://trak.in/Tags/Business/category/mobile-2/',
                      'https://trak.in/Tags/Business/category/india-business-opportunities-services-making-money/news/',
                      'https://trak.in/Tags/Business/category/india-business-opportunities-services-making-money/' +
                      'indi-business-news-everything-you-want-to-know-about-india/',
                      'https://trak.in/Tags/Business/category/india-business-opportunities-services-making-money/aviation-2/',
                      'https://trak.in/Tags/Business/category/india-business-opportunities-services-making-money/auto/',
                      'https://trak.in/Tags/Business/category/buzzing-indian-news/',
                      'https://trak.in/disclaimer/'
                      ]

    nonContentStrings = ['trak.in/cdn-cgi/l/email-protection',
                         'trak.in/wp-login.php']

    # never fetch URLs containing these strings:
    invalidURLSubStrings = []

    # write regexps in three groups ()()() so that the third group
    urlUniqueRegexps = [r'(http.+\/\/)(trak.in\/.+\-)([0-9]{5,})(/)']

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    articleDateRegexps = {
        r'(<meta property=\"article:published_time\" content=\")' +
        r'(20[0-9]{2}\-[0-9]{1,2}\-[0-9]{1,2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+00:00\")': "%Y-%m-%dT%H:%M:%S"
        }

    invalidTextStrings = []
    subStringsToFilter = []
    allowedDomains = ['trak.in']

    articleIndustryRegexps = []

    authorRegexps = [r'(<span class=\"post-author-name\">By <b>)([a-zA-Z0-9 _\-\.]{3,})(<\/b><\/span>)']

    # members used by functions of the class:
    authorMatchPatterns = []
    urlMatchPatterns = []
    dateMatchPatterns = dict()
    listOfURLS = []

    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(super().articleDateRegexps)
        self.urlUniqueRegexps = super().urlUniqueRegexps + self.urlUniqueRegexps
        super().__init__()

    # Special function for this plugin
    def extractUniqueIDFromContent(self, htmlContent, URLToFetch):
        """ Identify Unique ID From content
        Pattern: <link rel='shortlink' href='https://trak.in/?p=119415' />
        """
        uniqueString = ""
        crcValue = "zzz-zzz-zzz"
        if type(htmlContent) == bytes:
            htmlContent = htmlContent.decode('UTF-8')
        try:
            # calculate CRC string if url are not usable:
            crcValue = str(calculateCRC32(URLToFetch.encode('utf-8')))
            uniqueString = crcValue
        except Exception as e:
            logger.error("%s: When calculating CRC32 of URL: %s , URL was: %s",
                         self.pluginName,
                         e,
                         URLToFetch.encode('ascii'))
        if len(htmlContent) > self.minArticleLengthInChars:
            uniquePattern = re.compile(r"(<link rel='shortlink' href='https:\/\/trak.in\/\?p=)([0-9]{4,})(' \/>)")
            try:
                result = uniquePattern.search(htmlContent)
                if result is not None:
                    uniqueString = result.group(2)
            except Exception as e:
                logger.debug("%s: Unable to identify unique ID, error is: %s , Pattern: %s",
                             self.pluginName,
                             e,
                             uniquePattern)
        else:
            logger.warning("Invalid content found when trying to identify unique ID")
            raise ScrapeError("Invalid article since it does not have a unique identifier.")
        if uniqueString == crcValue:
            logger.info("%s: Unable to identify unique ID for article, hence using CRC32 code: %s",
                        self.pluginName,
                        uniqueString)
            raise ScrapeError("Invalid article since it does not have a unique identifier.")
        return(uniqueString)

    def extractIndustries(self, uRLtoFetch, htmlText):
        """ Extract the industry of the articles from its URL or contents
        """
        industries = []
        if type(htmlText) == bytes:
            htmlText = htmlText.decode('UTF-8')
        logger.debug("Extracting industries identified by the article.")
        try:
            industryPattern = r"(<meta property=\"article:tag\" content=\")([a-zA-Z0-9 \-_\.]{2,})(\" \/>)"
            results = re.findall(industryPattern, htmlText)
            for result in results:
                industries.append(result[1])
        except Exception as e:
            logger.debug("%s: Unable to extract industries, error is: %s , Pattern: %s",
                         self.pluginName,
                         e,
                         industryPattern)
        return(industries)

    def extractAuthors(self, htmlText):
        """ extract the author from the html content
        """
        authors = []
        authorStr = ""
        if type(htmlText) == bytes:
            htmlText = htmlText.decode('UTF-8')
        for authorMatch in self.authorMatchPatterns:
            logger.debug("Trying match pattern: %s", authorMatch)
            try:
                result = authorMatch.search(htmlText)
                if result is not None:
                    authorStr = result.group(2)
                    authors = authorStr.split(',')
                    # At this point, the text was correctly extracted, so exit the loop
                    break
            except Exception as e:
                logger.debug("Unable to identify the article authors: %s; string to parse: %s, URL: %s",
                             e, authorStr, self.URLToFetch)
        return(authors)

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
