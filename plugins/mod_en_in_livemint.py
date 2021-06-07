#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_livemint.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for LiveMint news portal


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
# from bs4 import BeautifulSoup
# import lxml
# import cchardet

# import this project's python libraries:
from base_plugin import basePlugin
# from scraper_utils import retainValidArticles
from data_structs import Types
from scraper_utils import deDupeList, filterRepeatedchars

##########

logger = logging.getLogger(__name__)


class mod_en_in_livemint(basePlugin):
    """ Web Scraping plugin - mod_en_in_livemint for the Live mint news portal
    """

    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = "https://www.livemint.com/latest-news"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ['https://www.livemint.com/rss/markets',
                     'https://www.livemint.com/rss/money']

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['www.livemint.com']

    # never fetch URLs containing these strings:
    invalidURLSubStrings = ['https://www.livemint.com/politics/news/',
                            'https://www.livemint.com/sports/',
                            'https://www.livemint.com/videos/',
                            'https://www.livemint.com/food/',
                            'https://www.livemint.com/how-to-lounge/art-culture/',
                            'https://www.livemint.com/food/cook/',
                            'https://www.livemint.com/how-to-lounge/',
                            'https://www.livemint.com/relationships/',
                            'livemint.com/fashion/',
                            'livemint.com/smart-living/',
                            'livemint.com/food/discover/',
                            'https://www.livemint.com/static/code-of-ethics',
                            'https://www.livemint.com/static/disclaimer',
                            'https://www.livemint.com/static/subscriber-tnc',
                            '/termsofuse.html',
                            '/contactus.html',
                            '/aboutus.html',
                            'mintiphone.page.link',
                            'mailto:',
                            'api.whatsapp.com'
                            ]

    nonContentStrings = ['/sitemapweb.html', '/Search/Link/Author/']

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://www.livemint.com/market',
                      'https://www.livemint.com/premium',
                      'https://www.livemint.com/mostpopular',
                      'https://www.livemint.com/wsj',
                      'https://www.livemint.com/companies/news',
                      'https://www.livemint.com/budget/news',
                      'https://www.livemint.com/notifications',
                      'https://www.livemint.com/auto-news',
                      'https://www.livemint.com/mint-lounge',
                      'https://www.livemint.com/topic/mint-insight',
                      'https://www.livemint.com/mutual-fund/mf-news',
                      'https://www.livemint.com/mint-lounge/business-of-life',
                      'https://www.livemint.com/brand-stories',
                      'https://www.livemint.com/topic/brand-masters',
                      'https://www.livemint.com/politics',
                      'https://www.livemint.com/news/opinion',
                      'https://www.livemint.com/opinion/',
                      'https://www.livemint.com/topic/mint-views-',
                      'https://www.livemint.com/news/talking-point',
                      'https://www.livemint.com/brand-post',
                      'https://www.livemint.com/technology/gadgets',
                      'https://www.livemint.com/technology',
                      'https://www.livemint.com/market/commodities',
                      'https://www.livemint.com/market/ipo',
                      'https://www.livemint.com/money/ask-mint-money',
                      'https://www.livemint.com/mint-lounge/features',
                      'https://www.livemint.com/opinion/online-views',
                      'https://www.livemint.com/opinion/blogs',
                      'https://www.livemint.com/topic/long-story-short',
                      'https://www.livemint.com/topic/money-with-monika',
                      'https://www.livemint.com/Markets/Cryptocurrency',
                      'https://www.livemint.com/Search/Link/Author/Ayushman-Baruah',
                      'https://www.livemint.com/apps',
                      'https://www.livemint.com/budget',
                      'https://www.livemint.com/budget/expectations',
                      'https://www.livemint.com/budget/opinion',
                      'https://www.livemint.com/companies/company-results',
                      'https://www.livemint.com/companies/news',
                      'https://www.livemint.com/companies/people',
                      'https://www.livemint.com/companies/start-ups',
                      'https://www.livemint.com/education',
                      'https://www.livemint.com/ifsc-code',
                      'https://www.livemint.com/industry/agriculture',
                      'https://www.livemint.com/industry/banking',
                      'https://www.livemint.com/industry/energy',
                      'https://www.livemint.com/industry/infotech',
                      'https://www.livemint.com/industry/infrastructure',
                      'https://www.livemint.com/industry/manufacturing',
                      'https://www.livemint.com/industry/retail',
                      'https://www.livemint.com/market/live-blog',
                      'https://www.livemint.com/market/mark-to-market',
                      'https://www.livemint.com/market/market-stats',
                      'https://www.livemint.com/market/stock-market-news',
                      'https://www.livemint.com/markets/mark-to-market',
                      'https://www.livemint.com/topic/long-story',
                      'https://www.livemint.com/topic/coronavirus',
                      'https://www.livemint.com/mint-lounge',
                      'https://www.livemint.com/mint-lounge/business-of-life',
                      'https://www.livemint.com/money',
                      'https://www.livemint.com/money/personal-finance',
                      'https://www.livemint.com/mostpopular',
                      'https://www.livemint.com/msitesearch',
                      'https://www.livemint.com/mutual-fund',
                      'https://www.livemint.com/myreads',
                      'https://www.livemint.com/newsletters',
                      'https://www.livemint.com/opinion/columns',
                      'https://www.livemint.com/politics',
                      'https://www.livemint.com/technology/apps',
                      'https://www.livemint.com/technology/gadgets',
                      'https://www.livemint.com/technology/tech-reviews',
                      'https://www.livemint.com/topic/5g-tech',
                      'https://www.livemint.com/topic/annual-banking-conclave',
                      'https://www.livemint.com/topic/business-of-entertainment',
                      'https://www.livemint.com/topic/digital-gurus',
                      'https://www.livemint.com/topic/foldable-smartphones',
                      'https://www.livemint.com/topic/long-reads',
                      'https://www.livemint.com/topic/market-analysis',
                      'https://www.livemint.com/topic/mint-50-top-mutual-funds',
                      'https://www.livemint.com/topic/mint-explainer',
                      'https://www.livemint.com/topic/mint-insight',
                      'https://www.livemint.com/topic/mint-views-',
                      'https://www.livemint.com/topic/plain-facts',
                      'https://www.livemint.com/topic/primer',
                      'https://www.livemint.com/topic/start-up-diaries',
                      'https://www.livemint.com/topic/why-not-mint-money'
                      'https://www.livemint.com/topic',
                      'https://www.livemint.com/topic/coronavirus',
                      'https://www.livemint.com/topic/long-story'
                      ]

    # write regexps in three groups ()()() so that the third group
    # gives a unique identifier such as a long integer at the end of a URL
    urlUniqueRegexps = [r'(https\/\/)(www\.livemint\.com\/.+\-)([0-9]{5,})(\.html)']

    # write the following regexps dict with each key as regexp to match the required date text,
    # group 2 of this regular expression should match the date string
    articleDateRegexps = dict()

    invalidTextStrings = []
    subStringsToFilter = ['Subscribe to Mint Newsletters',
                          'Enter a valid email',
                          'Thank you for subscribing to our newsletter.']

    allowedDomains = ['www.livemint.com']

    articleIndustryRegexps = []

    authorRegexps = []

    # members used by functions of the class:
    authorMatchPatterns = []
    urlMatchPatterns = []

    # fix this:
    # https://www.livemint.com/health/wellness/why-cannabis-customers-are-everyone-s-dream-demographic-111618299950296.html
    dateMatchPatterns = dict()
    listOfURLS = []

    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(basePlugin.articleDateRegexps)
        self.urlUniqueRegexps = self.urlUniqueRegexps + super().urlUniqueRegexps
        super().__init__()

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

    def extractArticleBody(self, htmlContent):
        """ extract the text body of the article
        """
        body_text = ""
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
