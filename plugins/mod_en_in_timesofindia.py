#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_timesofindia.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for the Times of India (TOI) blog
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
# #########

# import standard python libraries:
import logging
import re
# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup

from data_structs import Types, ScrapeError
from scraper_utils import calculateCRC32, deDupeList, filterRepeatedchars
from base_plugin import basePlugin

# #########

logger = logging.getLogger(__name__)


class mod_en_in_timesofindia(basePlugin):
    """ mod_en_in_timesofindia - Web Scraping plugin For Times of India blog
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    mainURL = 'https://timesofindia.indiatimes.com/blogs/business/'
    all_rss_feeds = ["https://timesofindia.indiatimes.com/blogs/feed/defaultrss"]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['https://timesofindia.indiatimes.com/blogs/']

    # never fetch these URLs:
    invalidURLSubStrings = []

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://timesofindia.indiatimes.com/blogs/',
                      'https://timesofindia.indiatimes.com/blogs/times-views/',
                      'https://timesofindia.indiatimes.com/blogs/toi-editorials/',
                      'https://timesofindia.indiatimes.com/blogs/city/',
                      'https://timesofindia.indiatimes.com/blogs/city/mumbai-city/',
                      'https://timesofindia.indiatimes.com/blogs/city/chennai-city/',
                      'https://timesofindia.indiatimes.com/blogs/city/bangalore-city/',
                      'https://timesofindia.indiatimes.com/blogs/city/delhi-city/',
                      'https://timesofindia.indiatimes.com/blogs/city/hyderabad-city/',
                      'https://timesofindia.indiatimes.com/blogs/india/',
                      'https://timesofindia.indiatimes.com/blogs/world/',
                      'https://timesofindia.indiatimes.com/blogs/entertainment/',
                      'https://timesofindia.indiatimes.com/blogs/sports/',
                      'https://timesofindia.indiatimes.com/blogs/spirituality-2/',
                      'https://timesofindia.indiatimes.com/blogs/business/',
                      'https://timesofindia.indiatimes.com/blogs/business/economy/',
                      'https://timesofindia.indiatimes.com/blogs/business/markets/',
                      'https://timesofindia.indiatimes.com/blogs/business/companies/',
                      'https://timesofindia.indiatimes.com/blogs/business/finance/',
                      'https://timesofindia.indiatimes.com/blogs/business/wealth/',
                      'https://timesofindia.indiatimes.com/blogs/environment/',
                      'https://timesofindia.indiatimes.com/blogs/lifestyle/',
                      'https://timesofindia.indiatimes.com/blogs/qna/',
                      'https://timesofindia.indiatimes.com/blogs/foreign-media/',
                      'https://timesofindia.indiatimes.com/blogs/tech/',
                      'https://timesofindia.indiatimes.com/blogs/science/',
                      'https://timesofindia.indiatimes.com/blogs/reviews/',
                      'https://timesofindia.indiatimes.com/blogs/author/dr-muneer/',
                      'https://timesofindia.indiatimes.com/blogs/the-medici-way/',
                      'https://timesofindia.indiatimes.com/blogs/author/abhishek-sikdar/',
                      'https://timesofindia.indiatimes.com/blogs/economic-update/',
                      'https://timesofindia.indiatimes.com/blogs/author/karandeep-sheoran/',
                      'https://timesofindia.indiatimes.com/blogs/voices/',
                      'https://timesofindia.indiatimes.com/blogs/author/quick-edit/',
                      'https://timesofindia.indiatimes.com/blogs/politics/',
                      'https://timesofindia.indiatimes.com/blogs/author/dinesh-khara/',
                      'https://timesofindia.indiatimes.com/blogs/author/devanshunarang/',
                      'https://timesofindia.indiatimes.com/blogs/dev-vani/fight-covid-today/',
                      'https://timesofindia.indiatimes.com/blogs/dev-vani/',
                      'https://timesofindia.indiatimes.com/blogs/author/dr-surabhi-singh/',
                      'https://timesofindia.indiatimes.com/blogs/marketing-swan/',
                      'https://timesofindia.indiatimes.com/blogs/author/reshmidasgupta/',
                      'https://timesofindia.indiatimes.com/blogs/SilkStalkings/',
                      'https://timesofindia.indiatimes.com/blogs/edit-page/',
                      'https://timesofindia.indiatimes.com/blogs/author/prof-mg-chandrakanth/',
                      'https://timesofindia.indiatimes.com/blogs/economic-policy/',
                      'https://timesofindia.indiatimes.com/blogs/author/team-toi/',
                      'https://timesofindia.indiatimes.com/blogs/edit-page/?ch=toi',
                      'https://timesofindia.indiatimes.com/blogs/author/et-edit/',
                      'https://timesofindia.indiatimes.com/blogs/et-editorials/',
                      'https://timesofindia.indiatimes.com/blogs/author/jamesmckew/',
                      'https://timesofindia.indiatimes.com/blogs/author/anita-inder-singh/',
                      'https://timesofindia.indiatimes.com/blogs/toi-edit-page/',
                      'https://timesofindia.indiatimes.com/blogs/author/samir-sathe/',
                      'https://timesofindia.indiatimes.com/blogs/business/page/549/',
                      'https://timesofindia.indiatimes.com/blogs/business/page/2/',
                      'https://timesofindia.indiatimes.com/blogs/author/arnabray/',
                      'https://timesofindia.indiatimes.com/blogs/author/indrajithazra/',
                      'https://timesofindia.indiatimes.com/blogs/author/saswato-r-das/',
                      'https://timesofindia.indiatimes.com/blogs/author/devi-shetty/',
                      'https://timesofindia.indiatimes.com/blogs/tag/narendra-modi/',
                      'https://timesofindia.indiatimes.com/blogs/tag/india-2/',
                      'https://timesofindia.indiatimes.com/blogs/tag/bjp/',
                      'https://timesofindia.indiatimes.com/blogs/tag/congress/',
                      'https://timesofindia.indiatimes.com/blogs/tag/china/',
                      'https://timesofindia.indiatimes.com/blogs/tag/pakistan/',
                      'https://timesofindia.indiatimes.com/blogs/tag/rbi/',
                      'https://timesofindia.indiatimes.com/blogs/tag/rahul-gandhi/',
                      'https://timesofindia.indiatimes.com/blogs/tag/donald-trump/',
                      'https://timesofindia.indiatimes.com/blogs/tag/supreme-court/',
                      'https://timesofindia.indiatimes.com/blogs/tag/us/',
                      'https://timesofindia.indiatimes.com/blogs/tag/modi/',
                      'https://timesofindia.indiatimes.com/blogs/tag/delhi/',
                      'https://timesofindia.indiatimes.com/blogs/tag/gst/',
                      'https://timesofindia.indiatimes.com/blogs/tag/cricket/',
                      'https://timesofindia.indiatimes.com/blogs/tag/gdp/',
                      'https://timesofindia.indiatimes.com/blogs/tag/economy-tag/',
                      'https://timesofindia.indiatimes.com/blogs/tag/arvind-kejriwal/',
                      'https://timesofindia.indiatimes.com/blogs/tag/covid-19/',
                      'https://timesofindia.indiatimes.com/blogs/tag/aap/',
                      'https://timesofindia.indiatimes.com/blogs/tag/demonetisation/',
                      'https://timesofindia.indiatimes.com/blogs/tag/bollywood/'
                      ]

    nonContentStrings = []

    urlUniqueRegexps = []

    invalidTextStrings = []
    subStringsToFilter = []
    articleDateRegexps = dict()
    authorRegexps = []
    dateMatchPatterns = dict()
    urlMatchPatterns = []
    authorMatchPatterns = []

    allowedDomains = ["timesofindia.indiatimes.com"]
    listOfURLS = []
    uRLdata = dict()
    urlMatchPatterns = []

    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(super().articleDateRegexps)
        self.urlUniqueRegexps = self.urlUniqueRegexps + super().urlUniqueRegexps
        super().__init__()

    # Special function for this plugin
    def extractUniqueIDFromContent(self, htmlContent, URLToFetch):
        """ Identify Unique ID From content
        Pattern: data-articlemsid="154959"
                 data-articlemsid="143505"
        """
        uniqueString = ""
        crcValue = "zzz-zzz-zzz"
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
            uniquePattern = re.compile(r"(data\-articlemsid=\")([0-9]{3,})(\")")
            try:
                result = uniquePattern.search(htmlContent.decode('UTF-8'))
                if result is not None:
                    uniqueString = result.group(2)
            except Exception as e:
                logger.debug("%s: Unable to identify unique ID, error is: %s , Pattern: %s",
                             self.pluginName,
                             e,
                             uniquePattern)
            return(uniqueString)
        else:
            logger.warning("%s: Invalid content found when trying to identify unique ID", URLToFetch)
        if uniqueString == crcValue:
            logger.warning("%s: Unable to identify unique ID for article, hence using CRC32 code: %s",
                           self.pluginName,
                           uniqueString)
        raise ScrapeError("Invalid article since it does not have a unique identifier.")

    def extractArticleBody(self, htmlContent):
        """ Extract the text body of the article
        """
        body_text = ""
        try:
            logger.debug("Extracting article content.")
            docRoot = BeautifulSoup(htmlContent, 'lxml')
            if len(docRoot.find_all("div", attrs={"class": "main-content single-article-content"})) > 0:
                body_root = docRoot.find_all("div", attrs={"class": "main-content single-article-content"})
                paragraphs = body_root[0].find_all("p")
                for para in paragraphs:
                    for child in para.children:
                        print(child)
                        body_text = body_text + child.strip()
        except Exception as e:
            logger.error("Error extracting article content: %s", e)
        return(body_text)

    def extractIndustries(self, uRLtoFetch, htmlText):
        """ Extract the industry of the articles from its URL or contents
        """
        industries = []
        try:
            logger.debug("Extracting industries identified by the article.")
            docRoot = BeautifulSoup(htmlText, 'lxml')
            docRoot.findAll('div')
        except Exception as e:
            logger.error("Error extracting industries: %s", e)
        return(industries)

    def extractAuthors(self, htmlText):
        """ extract Authors/Agency/Source from html
        """
        # default authors are blank list:
        authors = []
        try:
            authorRegex = r"(\"author\":{\"@type\":\"Person\",\"name\":\")([a-zA-Z0-9 ]+)(\"})"
            authorPattern = re.compile(authorRegex)
            regResults = authorPattern.search(htmlText)
            if regResults is not None:
                authors.append(regResults[2])
        except Exception as e:
            logger.error("Error extracting news agent from text: %s", e)
        return(authors)

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
