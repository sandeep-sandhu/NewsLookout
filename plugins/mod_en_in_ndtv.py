#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_ndtv.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for NDTV


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
# import re

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup

# import this project's python libraries:
from base_plugin import basePlugin
from scraper_utils import retainValidArticles, deDupeList, filterRepeatedchars
from data_structs import Types

##########

logger = logging.getLogger(__name__)


class mod_en_in_ndtv(basePlugin):
    """ Web Scraping plugin: mod_en_in_ndtv
    For NDTV
    """
    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 250

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = "https://www.ndtv.com/business?pfrom=home-ndtv_header-globalnav"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ['https://feeds.feedburner.com/ndtvprofit-latest',
                     'https://feeds.feedburner.com/ndtvnews-latest']

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['www.ndtv.com/']

    nonContentStrings = ['/trends/most-popular-business-news',
                         'www.ndtv.com/business/?',
                         'www.ndtv.com/webstories/',
                         'www.ndtv.com/topic/',
                         '/photos/news/top-photos-of-the-day',
                         'www.ndtv.com/careers/?',
                         'www.ndtv.com/education/?',
                         'www.ndtv.com/?pfrom',
                         'www.ndtv.com/budget?pfrom',
                         'www.ndtv.com/webstories/celeb',
                         'www.ndtv.com/jobs/',
                         'www.ndtv.com/business/hindi'
                         ]

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://www.ndtv.com/',
                      'https://www.ndtv.com/topic/stock-talk',
                      'https://www.ndtv.com/business/marketdata',
                      'https://www.ndtv.com/business',
                      'https://www.ndtv.com/business/latest',
                      'https://www.ndtv.com/business/your-money',
                      'https://www.ndtv.com/business/corporates',
                      'https://www.ndtv.com/business/life-and-careers',
                      'https://www.ndtv.com/business/tech-media-telecom',
                      'https://www.ndtv.com/business/banking-finance',
                      'https://www.ndtv.com/business/consumer-products',
                      'https://www.ndtv.com/business/pharma',
                      'https://www.ndtv.com/business/energy',
                      'https://www.ndtv.com/business/infrastructure',
                      'https://www.ndtv.com/business/people',
                      'https://www.ndtv.com/business/economic-indicators',
                      'https://www.ndtv.com/business/economic-policy',
                      'https://www.ndtv.com/business/global-economy',
                      'https://www.ndtv.com/world-news?pfrom=',
                      'https://www.ndtv.com/business/industries',
                      'https://www.ndtv.com/business/auto',
                      'https://www.ndtv.com/business/earnings?pfrom=',
                      'https://www.ndtv.com/business/industries?pfrom=',
                      'https://www.ndtv.com/page/apps/?pfrom=',
                      'https://www.ndtv.com/shopping?pfrom=',
                      'https://www.ndtv.com/video?pfrom=',
                      'https://www.ndtv.com/beeps?pfrom',
                      'https://www.ndtv.com/webstories?pfrom=',
                      'https://www.ndtv.com/bengali',
                      'https://www.ndtv.com/tamil',
                      'https://www.ndtv.com/diaspora',
                      'https://www.ndtv.com/cheat-sheet',
                      'https://www.ndtv.com/webstories/beauty',
                      'https://www.ndtv.com/webstories/tech',
                      'https://www.ndtv.com/webstories/humor',
                      'https://www.ndtv.com/business/forex',
                      'https://www.ndtv.com/convergence/ndtv/new/NDTVNewsAlert.aspx',
                      'https://www.ndtv.com/convergence/ndtv/new/Complaint.aspx',
                      'https://www.ndtv.com/convergence/ndtv/new/feedback.aspx',
                      'https://www.ndtv.com/convergence/ndtv/new/disclaimer.aspx',
                      'https://www.ndtv.com/careers/'
                      'https://movies.ndtv.com',
                      'https://www.ndtv.com/business',
                      'https://food.ndtv.com',
                      'https://swachhindia.ndtv.com',
                      'https://doctor.ndtv.com',
                      'https://sports.ndtv.com',
                      'https://khabar.ndtv.com',
                      'https://gadgets.ndtv.com',
                      'https://swirlster.ndtv.com',
                      'https://www.ndtv.com/latest?pfrom=',
                      'https://www.ndtv.com/trends?pfrom=',
                      'https://www.ndtv.com/people?pfrom=',
                      'https://www.ndtv.com/science?pfrom=',
                      'https://www.ndtv.com/tv-schedule?pfrom=',
                      'https://www.ndtv.com/education?pfrom=',
                      'https://www.ndtv.com/india?pfrom=',
                      'https://www.ndtv.com/business/stocks',
                      'https://www.ndtv.com/business/futures-and-options',
                      'https://www.ndtv.com/business/currency',
                      'https://www.ndtv.com/business/global-markets',
                      'https://www.ndtv.com/business/savings-and-investments',
                      'https://www.ndtv.com/business/mutual-funds',
                      'https://www.ndtv.com/business/mutual-funds/mf-dashboard',
                      'https://www.ndtv.com/business/insurance',
                      'https://www.ndtv.com/business/tax',
                      'https://www.ndtv.com/photos',
                      'https://www.ndtv.com/telangana-news',
                      'https://www.ndtv.com/topic/uma-sudhir',
                      'https://www.ndtv.com/topic/telangana',
                      'https://www.ndtv.com/topic/telangana-pot-of-gold',
                      'https://www.ndtv.com/topic/gold-ornaments',
                      'https://www.ndtv.com/electons',
                      'https://www.ndtv.com/education/exam-notifications'
                      ]

    # never fetch URLs containing these strings:
    invalidURLSubStrings = ['/entertainment',
                            '/webstories/celeb/',
                            '/webstories/food',
                            'www.ndtv.com/video/',
                            'www.ndtv.com/entertainment?',
                            'www.ndtv.com/convergence/ndtv/new/codeofethics.aspx',
                            'www.ndtv.com/convergence/ndtv/new/disclaimer.aspx',
                            'www.ndtv.com/business/gadgets',
                            'www.ndtv.com/convergence/ndtv/new/dth.aspx',
                            'www.ndtv.com/sites/ureqa-epg-ott/',
                            'www.ndtv.com/convergence/ndtv/advertise/ndtv_leaderboard.aspx',
                            'www.ndtv.com/convergence/ndtv/corporatepage/index.aspx',
                            '/photos/entertainment/',
                            'khabar.ndtv.com',
                            'sports.ndtv.com',
                            'food.ndtv.com',
                            'movies.ndtv.com',
                            '/disclaimer.aspx',
                            'https://swachhindia.ndtv.com',
                            'https://doctor.ndtv.com',
                            'https://gadgets.ndtv.com',
                            'https://swirlster.ndtv.com'
                            ]

    # write regexps in three groups ()()() so that the third group
    urlUniqueRegexps = [r'(^http.+\/\/)(www.ndtv.com\/.+\-)([0-9]{5,})',
                        r'(^http.+\/\/)(www.ndtv.com\/.+\-)([0-9]{5,})(\?)']

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    articleDateRegexps = {
        r"(content = \")(20[0-9]{2}\-[0-9]{2}\-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\+05:30\")": "%Y-%m-%dT%H:%M:%S",
        r"(Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2} [0-9]{1,2}:[0-9]{2})( [a-zA-Z]{2} IST)": "%B %d, %Y %H:%M"
        # FIXME: Could not identify article date: time data 'Jun 7, 2021 5:47' does not match format '%B %d, %Y %H:%M',
        # string to parse: Jun 7, 2021 5:47, using regexp:
        # (Updated: )([a-zA-Z]+ [0-9]{1,2}, 20[0-9]{2} [0-9]{1,2}:[0-9]{2})( [a-zA-Z]{2} IST),
        # URL: https://www.ndtv.com/education/cancel-university-exams-say-students-states-scrap-board-exams
        }

    invalidTextStrings = []
    subStringsToFilter = []
    allowedDomains = ["www.ndtv.com"]

    articleIndustryRegexps = []

    authorRegexps = []

    # members used by functions of the class:
    authorMatchPatterns = []
    urlMatchPatterns = []
    dateMatchPatterns = dict()
    listOfURLS = []

    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(basePlugin.articleDateRegexps)
        self.urlUniqueRegexps = self.urlUniqueRegexps + super().urlUniqueRegexps
        super().__init__()

    def getArticlesListFromRSS(self):
        """ Extract Article listing using the BeautifulSoup library
        to identify the list from its RSS feed
        """
        # <item>
        # <link><![CDATA[https://www.ndtv.com/business/sbi-readies-mutual-fund-venture-for-ipo-2379481]]></link>
        for thisFeedURL in self.all_rss_feeds:
            try:
                rawData = self.networkHelper.fetchRawDataFromURL(thisFeedURL, self.pluginName)
                rss_feed_xml = BeautifulSoup(rawData, 'lxml-xml')
                for item in rss_feed_xml.channel:
                    if item.name == "item":
                        link_contents = item.link.contents[0]
                        self.listOfURLS.append(link_contents)
            except Exception as e:
                logger.error("%s: Error getting urls listing from RSS feed %s: %s",
                             self.pluginName,
                             thisFeedURL,
                             e)
        self.listOfURLS = retainValidArticles(self.listOfURLS,
                                              self.validURLStringsToCheck)

    def extractArticleBody(self, htmlContent):
        """ Extract article's text using the Beautiful Soup library
        """
        body_text = ""
        try:
            # get article text data by parsing specific tags:
            docRoot = BeautifulSoup(htmlContent, 'lxml')
            section = docRoot.find_all(class_=['ins_storybody', 'content_text row description', 'fullstoryCtrl_fulldetails'])
            paragraphList = []
            for node in section:
                paragraphList = paragraphList + node.find_all('p', text=True)
            for item in paragraphList:
                body_text = body_text + str(item.get_text())
            section = docRoot.findAll('span', {"itemprop": 'articleBody'})
            if len(section) > 0:
                for item in section:
                    body_text = body_text + str(item.get_text())
            section = docRoot.findAll('div', {"itemprop": 'articleBody'})
            if len(section) > 0:
                for item in section:
                    body_text = body_text + str(item.get_text())
        except Exception as e:
            logger.error("Exception extracting article via tags: %s", e)
        return(body_text)

    def extractArticleTitle(self, htmlContent):
        """ Extract article's text using the Beautiful Soup library
        """
        title_text = ""
        try:
            # get article text data by parsing specific tags:
            docRoot = BeautifulSoup(htmlContent, 'lxml')
            section = docRoot.findAll('h1', {"itemprop": 'headline'})
            if len(section) > 0:
                for item in section:
                    title_text = title_text + str(item.get_text())
        except Exception as e:
            logger.error("Exception extracting article via tags: %s", e)
        return(title_text)

    def extractIndustries(self, uRLtoFetch, htmlText):
        """ Extract the industry of the articles from its URL or contents
        """
        industries = []
        return(industries)

    def extractAuthors(self, htmlText):
        """ extract the author from the html content
        """
        authors = []
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
