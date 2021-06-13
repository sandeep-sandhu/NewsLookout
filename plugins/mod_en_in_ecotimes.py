#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_ecotimes.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for the Economic Times news portal
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu

 Provides:
    mod_en_in_ecotimes
        extractIndustries
        extractAuthors
        extractPublishedDate
        extractArticleBody
        extractArticleBodyFormat1
        extractArticleBodyFormat2
        extractArticleBodyFormat3
        extractArticleBodyFormat4
        extractUniqueIDFromURL


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
from datetime import datetime
# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup
# import lxml
# import cchardet

# import this project's python libraries:
from base_plugin import basePlugin
from scraper_utils import getNetworkLocFromURL, filterRepeatedchars, deDupeList
from data_structs import Types

##########

logger = logging.getLogger(__name__)


class mod_en_in_ecotimes(basePlugin):
    """ Web Scraping plugin: mod_en_in_ecotimes
    For Economic times Newspaper
    Language: English
    Country: India
    """

    minArticleLengthInChars = 400

    pluginType = Types.MODULE_NEWS_CONTENT  # implies web-scraper for news content

    mainURL = 'https://economictimes.indiatimes.com/industry'

    # rss_feed = "https://economictimes.indiatimes.com/rssfeedsdefault.cms"
    all_rss_feeds = ['https://economictimes.indiatimes.com/rssfeedsdefault.cms',
                     'https://economictimes.indiatimes.com/rssfeedstopstories.cms',
                     'https://economictimes.indiatimes.com/news/latest-news/rssfeeds/20989204.cms'
                     ]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['economictimes.indiatimes.com/']

    # never fetch URLs containing these strings:
    invalidURLSubStrings = ['//www.indiatimes.com/',
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
                            'economictimes.indiatimes.com/privacyacceptance.cms'
                            ]

    # get URL links from URLs containing these strings, but don't fetch content from them:
    nonContentStrings = ['economictimes.indiatimes.com/marketstats/pid-',
                         '/indexsummary/indexid-',
                         'economictimes.indiatimes.com/news/elections/',
                         'economictimes.indiatimes.com/primearticlelist/',
                         '/articlelist/',
                         'economictimes.indiatimes.com/markets/stocks/stock-quotes?',
                         'economictimes.indiatimes.com/?',
                         'economictimes.indiatimes.com/etlatestnews.cms?',
                         'economictimes.indiatimes.com/mostemailed.cms?',
                         'economictimes.indiatimes.com/mostcommented.cms?'
                         ]

    # get URL links from these URLs but don't fetch content from them:
    nonContentURLs = [mainURL,
                      'https://economictimes.indiatimes.com/marketstats/'
                      + 'duration-1d,marketcap-largecap,pageno-1,pid-0,sort-intraday,sortby-percentchange,sortorder-desc.cms',
                      'https://economictimes.indiatimes.com/marketstats/'
                      + 'pid-40,exchange-nse,sortby-value,sortorder-desc.cms',
                      'https://economictimes.indiatimes.com/marketstats/'
                      + 'pid-0,pageno-1,sortby-percentchange,sortorder-desc,sort-intraday.cms',
                      'https://economictimes.indiatimes.com/topic/restrictions',
                      'https://economictimes.indiatimes.com/topic/Indian-equity-market',
                      'https://economictimes.indiatimes.com/et500',
                      'https://economictimes.indiatimes.com/personal-finance',
                      'https://economictimes.indiatimes.com/mutual-funds',
                      'https://economictimes.indiatimes.com/amazon.cms',
                      'https://economictimes.indiatimes.com/breakingnewslist.cms',
                      'https://economictimes.indiatimes.com/environment',
                      'https://economictimes.indiatimes.com/bookmarkslist.cms',
                      'https://economictimes.indiatimes.com/news/latest-news/most-read',
                      'https://economictimes.indiatimes.com/news/latest-news/most-shared',
                      'https://economictimes.indiatimes.com/news/latest-news/most-commented',
                      'https://economictimes.indiatimes.com/news/economy',
                      'https://economictimes.indiatimes.com/topic/FII-inflow',
                      'https://economictimes.indiatimes.com/topic/FII-trends',
                      'https://economictimes.indiatimes.com/topic/FMCG-stocks',
                      'https://economictimes.indiatimes.com/news/coronavirus',
                      'https://economictimes.indiatimes.com/news/company',
                      'https://economictimes.indiatimes.com/tv',
                      'https://economictimes.indiatimes.com/topic/Covid-19',
                      'https://economictimes.indiatimes.com/topic/capital-goods-stocks',
                      'https://economictimes.indiatimes.com/sunday-et',
                      'https://economictimes.indiatimes.com/et-now',
                      'https://economictimes.indiatimes.com/et-now/auto',
                      'https://economictimes.indiatimes.com/et-now/brand-equity',
                      'https://economictimes.indiatimes.com/et-now/commodities',
                      'https://economictimes.indiatimes.com/et-now/corporate',
                      'https://economictimes.indiatimes.com/et-now/daily',
                      'https://economictimes.indiatimes.com/et-now/entertainment',
                      'https://economictimes.indiatimes.com/et-now/experts',
                      'https://economictimes.indiatimes.com/et-now/finance',
                      'https://economictimes.indiatimes.com/et-now/markets',
                      'https://economictimes.indiatimes.com/et-now/policy',
                      'https://economictimes.indiatimes.com/et-now/results',
                      'https://economictimes.indiatimes.com/et-now/stocks',
                      'https://economictimes.indiatimes.com/et-now/technology',
                      'https://economictimes.indiatimes.com/industry/auto',
                      'https://economictimes.indiatimes.com/industry/cons-products',
                      'https://economictimes.indiatimes.com/industry/energy',
                      'https://economictimes.indiatimes.com/industry/healthcare/biotech',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs',
                      'https://economictimes.indiatimes.com/industry/media/entertainment',
                      'https://economictimes.indiatimes.com/industry/services',
                      'https://economictimes.indiatimes.com/industry/telecom',
                      'https://economictimes.indiatimes.com/industry/transportation',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=1',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=2',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=3',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=4',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=5',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=6',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=7',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=8',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=9',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=a',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=b',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=c',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=d',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=e',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=f',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=g',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=h',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=i',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=j',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=k',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=l',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=m',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=o',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=p',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=q',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=r',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=s',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=t',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=u',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=v',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=w',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=x',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=y',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-quotes?ticker=z',
                      'https://economictimes.indiatimes.com/markets/nifty-crash',
                      'https://economictimes.indiatimes.com/markets/market-moguls',
                      'https://economictimes.indiatimes.com/markets/visualize.cms',
                      'https://economictimes.indiatimes.com/markets/technical-charts',
                      'https://economictimes.indiatimes.com/markets/commodities',
                      'https://economictimes.indiatimes.com/markets/live-coverage',
                      'https://economictimes.indiatimes.com/markets/ipo',
                      'https://economictimes.indiatimes.com/markets/forex/indian-rupee',
                      'https://economictimes.indiatimes.com/markets/forex',
                      'https://economictimes.indiatimes.com/markets/forex/forexnews',
                      'https://economictimes.indiatimes.com/markets/bonds',
                      'https://economictimes.indiatimes.com/markets/sebi',
                      'https://economictimes.indiatimes.com/markets/rbi',
                      'https://economictimes.indiatimes.com/mutual-fund-screener',
                      'https://economictimes.indiatimes.com/markets/candlestick-screener',
                      'https://economictimes.indiatimes.com/markets/stocks/mcalendar.cms',
                      'https://economictimes.indiatimes.com/markets/gst-collection',
                      'https://economictimes.indiatimes.com/markets/stocks/earnings',
                      'https://economictimes.indiatimes.com/topic/stock-market-news',
                      'https://economictimes.indiatimes.com/topic/sebi',
                      'https://economictimes.indiatimes.com/indices/nifty_50_companies',
                      'https://economictimes.indiatimes.com/sitemap_makets.cms',
                      'https://economictimes.indiatimes.com/commoditylisting.cms?head=',
                      'https://chartmantra.economictimes.indiatimes.com/GameBoard.htm',
                      'https://economictimes.indiatimes.com/talkingheads.cms',
                      'https://economictimes.indiatimes.com/definition',
                      'https://economictimes.indiatimes.com/podcast',
                      'https://economictimes.indiatimes.com/mobile',
                      'https://economictimes.indiatimes.com/news/indian-navy-news',
                      'https://economictimes.indiatimes.com/news/india-unlimited/csr',
                      'https://economictimes.indiatimes.com/news/irctc-news',
                      'https://economictimes.indiatimes.com/nri',
                      'https://economictimes.indiatimes.com/opinion',
                      'https://economictimes.indiatimes.com/markets/stocks/liveblog',
                      'https://economictimes.indiatimes.com/markets/stocks/etmarkets-podcasts',
                      'https://economictimes.indiatimes.com/news/russia-covid-vaccine',
                      'https://economictimes.indiatimes.com/panache',
                      'https://economictimes.indiatimes.com/small-biz/topmsmes',
                      'https://economictimes.indiatimes.com/spotlight/greatmanagerawards.cms',
                      'https://economictimes.indiatimes.com/stock-price-alerts/pricealerts.cms',
                      'https://economictimes.indiatimes.com/tech',
                      'https://economictimes.indiatimes.com/tech/funding',
                      'https://economictimes.indiatimes.com/tech/startups',
                      'https://economictimes.indiatimes.com/tech/tech-bytes',
                      'https://economictimes.indiatimes.com/tomorrowmakers.cms',
                      'https://economictimes.indiatimes.com/topic/infrastructure',
                      'https://economictimes.indiatimes.com/topic/order-book',
                      'https://economictimes.indiatimes.com/topic/input-costs',
                      'https://economictimes.indiatimes.com/topic/L&T-outlook',
                      'https://economictimes.indiatimes.com/topic/Q4-earnings',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-screener/GARP',
                      'https://economictimes.indiatimes.com/markets/midcap-stocks',
                      'https://economictimes.indiatimes.com/markets/smallcap-stocks',
                      'https://economictimes.indiatimes.com/markets/largecap-stocks',
                      'https://economictimes.indiatimes.com/markets/stocks/stock-market-holiday-calendar',
                      'https://economictimes.indiatimes.com/industry/banking/finance',
                      'https://economictimes.indiatimes.com/industry/banking/finance/banking',
                      'https://economictimes.indiatimes.com/industry/banking-/-finance/banking',
                      'https://economictimes.indiatimes.com/industry/cons-products/durables',
                      'https://economictimes.indiatimes.com/industry/cons-products/electronics',
                      'https://economictimes.indiatimes.com/industry/cons-products/fashion-/-cosmetics-/-jewellery',
                      'https://economictimes.indiatimes.com/industry/cons-products/fmcg',
                      'https://economictimes.indiatimes.com/industry/cons-products/food',
                      'https://economictimes.indiatimes.com/industry/cons-products/garments-/-textiles',
                      'https://economictimes.indiatimes.com/industry/cons-products/liquor',
                      'https://economictimes.indiatimes.com/industry/cons-products/paints',
                      'https://economictimes.indiatimes.com/industry/cons-products/tobacco',
                      'https://economictimes.indiatimes.com/industry/energy/power',
                      'https://economictimes.indiatimes.com/industry/energy/oil-gas',
                      'https://economictimes.indiatimes.com/industry/renewables',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/construction',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/engineering',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/cement',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/chem-/-fertilisers',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/metals-mining',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/packaging',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/paper-/-wood-/-glass/-plastic/-marbles',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/petrochem',
                      'https://economictimes.indiatimes.com/industry/indl-goods/svs/steel',
                      'https://economictimes.indiatimes.com/industry/healthcare-/-biotech/biotech',
                      'https://economictimes.indiatimes.com/industry/healthcare/biotech/healthcare',
                      'https://economictimes.indiatimes.com/industry/healthcare/biotech/pharmaceuticals',
                      'https://economictimes.indiatimes.com/industry/services/advertising',
                      'https://economictimes.indiatimes.com/industry/services/consultancy-/-audit',
                      'https://economictimes.indiatimes.com/industry/services/education',
                      'https://economictimes.indiatimes.com/industry/services/hotels-/-restaurants',
                      'https://economictimes.indiatimes.com/industry/services/property-/-cstruction',
                      'https://economictimes.indiatimes.com/industry/services/retail',
                      'https://economictimes.indiatimes.com/industry/services/travel',
                      'https://economictimes.indiatimes.com/industry/media-/-entertainment/entertainment',
                      'https://economictimes.indiatimes.com/industry/transportation/railways',
                      'https://economictimes.indiatimes.com/industry/transportation/airlines-/-aviation',
                      'https://economictimes.indiatimes.com/industry/transportation/shipping-/-transport',
                      'https://economictimes.indiatimes.com/tech/information-tech',
                      'https://economictimes.indiatimes.com/tech/technology',
                      'https://economictimes.indiatimes.com/industry/jiopages',
                      'https://economictimes.indiatimes.com/markets/global-markets',
                      'https://economictimes.indiatimes.com/markets/sgx-nifty',
                      'https://economictimes.indiatimes.com/indices/sensex_30_companies',
                      'https://timesofindia.indiatimes.com/blogs/author/dr-arun-singh-and-neeraj-sahai/',
                      'https://timesofindia.indiatimes.com/blogs/author/rajivkumar/',
                      'https://timesofindia.indiatimes.com/blogs/author/bachikarkaria/',
                      'https://timesofindia.indiatimes.com/blogs/author/manoj-joshi/',
                      'https://timesofindia.indiatimes.com/blogs/author/vinitadawranangia/',
                      'https://timesofindia.indiatimes.com/blogs/author/chetanbhagat/',
                      'https://timesofindia.indiatimes.com/blogs/author/ruchirsharma/',
                      'https://timesofindia.indiatimes.com/blogs/author/bachikarkaria/',
                      'https://timesofindia.indiatimes.com/blogs/author/manoj-joshi/',
                      'https://timesofindia.indiatimes.com/blogs/author/vinitadawranangia/',
                      'https://timesofindia.indiatimes.com/blogs/author/chetanbhagat/',
                      'https://timesofindia.indiatimes.com/blogs/author/ruchirsharma/'
                      ]

    urlUniqueRegexps = [r"(http.+\/economictimes\.indiatimes\.com)(.*\/)([0-9]+)(\.cms)",
                        r"(\.economictimes\.indiatimes\.com\/)(.+\/)([0-9]+)",
                        r'(http.+\/\/)(.+economictimes\.indiatimes\.com\/.+\/)([0-9]{5,})'
                        ]

    articleDateRegexps = {
          r"(<meta http-equiv=\"Last-Modified\" content=\"[a-zA-Z]{3,}, )"
          + r"([a-zA-Z]{3}[ ]+[0-9]{1,2},[ ]+20[0-9]{2}[ ]+[0-9]{1,2}:[0-9]{2}:[0-9]{2})( [AMPamp]{2}\"\/>)":
          "%b %d, %Y  %H:%M:%S"
          }

    dateMatchPatterns = dict()

    articleIndustryRegexps = [r"(data-category-name = ')([a-zA-Z0-9 \-,]+)(')"]

    authorRegexps = [r"(agency:')([a-zA-Z0-9]+)(')",
                     r"(channel :  ')([a-zA-Z0-9]+)(',)",
                     r"(agename = ')([a-zA-Z0-9]+)(';)",
                     r"(<div class = \"ag tac\">)([a-zA-Z0-9]+)(<\/div>)",
                     r"(\"publisher\":{\"@type\":\"Organization\",\"name\":\")([a-zA-Z0-9]+)(\")",
                     r"(\.economictimes\.indiatimes\.com\/agency\/.+\" target = \"_blank\">)([a-zA-Z0-9]+)(<\/a>)"
                     ]

    authorMatchPatterns = []

    urlMatchPatterns = []

    invalidTextStrings = ["If you choose to ignore this message, we'll assume that you are happy to receive all cookies"]

    subStringsToFilter = []

    allowedDomains = ["economictimes.indiatimes.com"]
    listOfURLS = []
    uRLdata = dict()

    def __init__(self):
        """ Initialize the object
        Use base class's lists and dicts in searching for unique url and published date strings
        """
        self.articleDateRegexps.update(basePlugin.articleDateRegexps)
        self.urlUniqueRegexps = self.urlUniqueRegexps + super().urlUniqueRegexps
        super().__init__()

    def extractArchiveURLLinksForDate(self, runDate):
        """ Extracting archive URL links for given date
        Day no is calculated using Excel logic, it is days since 31-Dec-1899
        For example, the archive URL for 1 Jan, 2015 is:
         https://economictimes.indiatimes.com/archivelist/year-2015,month-1,starttime-42005.cms
        URL for 13 Oct, 2015 is:
         https://economictimes.indiatimes.com/archivelist/year-2015,month-10,starttime-42290.cms
        URL for 30-nov-2018 is:
         https://economictimes.indiatimes.com/archivelist/year-2018,month-11,starttime-43434.cms
        """
        resultSet = []
        searchResultsURLForDate = None
        try:
            startDate = datetime.strptime('1899-12-31', '%Y-%m-%d')
            if type(runDate).__name__ == 'datetime':
                dateDiff = runDate - startDate
            elif type(runDate).__name__ == 'str':
                runDate = datetime.strptime(runDate, '%Y-%m-%d')
                dateDiff = runDate - startDate
            else:
                return([])
            archiveDayNo = dateDiff.days + 1
            yearNo = runDate.strftime('%Y')
            monthNo = runDate.strftime('%m')
            searchResultsURLForDate = ('https://economictimes.indiatimes.com/archivelist/year-' + str(yearNo) +
                                       ',month-' + str(monthNo) +
                                       ',starttime-' + str(archiveDayNo) +
                                       '.cms')
            URLsListForDate = self.extractLinksFromURLList(runDate, [searchResultsURLForDate])
            if URLsListForDate is not None and len(URLsListForDate) > 0:
                resultSet = URLsListForDate
            logger.debug("%s: Added URLs for current date from archive page at: %s",
                         self.pluginName,
                         searchResultsURLForDate)
        except Exception as e:
            logger.error("%s: Error extracting archive URL links for given date %s: %s; URL = %s",
                         self.pluginName, runDate, e, searchResultsURLForDate)
        return(resultSet)

    def extractIndustries(self, uRLtoFetch, htmlText):
        """  Extract Industries from text and URL
        """
        industries = []
        try:
            logger.debug("Attempting to extract industries identified by the article.")
            # first identify network location of the URL:
            netwLocation = getNetworkLocFromURL(uRLtoFetch).split('.')
            # use the smallest sub-domain:
            if not ('economictimes' == netwLocation[0]):
                industries.append(netwLocation[0])
            # else:
            #    docRoot = BeautifulSoup(htmlText, 'lxml')
            # TODO: parse html and try to identify the industry
        except Exception as e:
            logger.error("Error identifying industries for URL %s: %s",
                         uRLtoFetch.encode("ascii", "ignore"),
                         e)
        return(industries)

    def extractAuthors(self, htmlText):
        """ Extract Authors/Agency/Source of the article from its raw html code
        """
        authors = []
        authorStr = None
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
                logger.debug("Unable to identify the article authors using regex: %s; string to parse: %s, URL: %s",
                             e, authorStr, self.URLToFetch)
        if authorStr is None:
            authors = self.extractAuthorsFromTags(htmlText)
        return(authors)

    def extractAuthorsFromTags(self, htmlText):
        """ Extract Authors/Agency/Source of the article from its raw html code
        """
        authors = []
        logger.debug("Re-attempting identifying authors for URL: %s", self.URLToFetch)
        try:
            docRoot = BeautifulSoup(htmlText, 'lxml')
            body_root = docRoot.find("span", "ag")
            if body_root is not None:
                if len(body_root.getText()) < 1:
                    if body_root.find("img") is None:
                        authors = []
                    else:
                        authors = [body_root.img['alt']]
                else:
                    authors = [body_root.getText()]
        except Exception as e:
            logger.error("Error on re-attempting identifying authors from tags: %s, URL: %s",
                         e, self.URLToFetch)
        return(authors)

    def extractArticleBody(self, htmlContent):
        """ Extract Article Text content using Beautiful Soup library
        """
        body_text = ""
        try:
            # get article text data by parsing article-body tag:
            docRoot = BeautifulSoup(htmlContent, 'lxml')
            body_text = self.extractArticleBodyFormat1(docRoot)
            # Try this for paywall: <article data-apw = "1" class = "artData clr paywall">
            if len(body_text) < 5 and len(docRoot.find_all("article", attrs={"class": "artData clr paywall"})) > 0:
                body_text = self.extractArticleBodyFormat2(docRoot)
            # Try this for blogs:
            if len(body_text) < 5 and len(docRoot.find_all("div", attrs={"class": "blog-show"})) > 0:
                body_text = self.extractArticleBodyFormat3(docRoot)
            # alternative format 4
            if len(body_text) < 5 and len(docRoot.find_all("article", attrs={"class": "artData clr "})) > 0:
                body_text = self.extractArticleBodyFormat4(docRoot)
            # alternative format 5
            if len(body_text) < 5:
                body_text = self.extractArticleBodyFormat5(docRoot)
            # except Warning as w:
            #    logger.warn("Warning when extracting text via BeautifulSoup: %s", w)
        except Exception as e:
            logger.error("Exception extracting article content via tags: %s", e)
        return(body_text)

    def extractArticleBodyFormat1(self, docRoot):
        """ Extract Article Text content in format 1
        """
        body_text = ""
        try:
            # get article text data by parsing article-body tag:
            body_root = docRoot.find_all("div",
                                         attrs={"itemprop": "mainContentOfPage",
                                                "class": "article-body"}
                                         )
            if len(body_root) > 0:
                firstTag = body_root[0]
                sub_section = firstTag.find_all("div", attrs={"class": "post-text artcle-txt article-type-news"})
                if len(sub_section) > 0:
                    sub_sub_section = sub_section[0].find_all("div", attrs={"class": "Normal"})

                    if len(sub_sub_section) > 0:
                        body_text = sub_sub_section[0].getText()
                        logger.debug("Successfully extracted article content in format 1")

        except Exception as e:
            logger.error("Extracting article content in format 1: %s", e)
        return(body_text)

    def extractArticleBodyFormat2(self, docRoot):
        """ Extract Article Text content in format 2
        """
        body_text = ""
        try:
            # only get contents if its a blog type of article:
            body_root = docRoot.find_all("article", attrs={"class": "artData clr paywall"})
            if len(body_root) > 0:
                firstTag = body_root[0]
                # <div data-brcount = "43" class = "artText medium">
                sub_section = firstTag.find_all("div", attrs={"class": "artText"})

                if len(sub_section) > 0:
                    body_text = sub_section[0].getText()
                    logger.debug("Successfully extracted article content in format 2")

        except Exception as e:
            logger.error("Extracting article content in format 2: %s", e)

        return(body_text)

    def extractArticleBodyFormat3(self, docRoot):
        """ Extract Article Text content in format 3
        """
        body_text = ""
        try:
            # only get contents if its a blog type of article:
            body_root = docRoot.find_all("div", attrs={"class": "main-content"})
            if len(body_root) > 0:
                firstTag = body_root[0]
                # get only <p> contents
                for paragraph in firstTag.children:
                    if paragraph.name == 'p':
                        body_text = body_text + paragraph.getText()

                logger.debug("Successfully extracted article content in format 3")

        except Exception as e:
            logger.error("Extracting article content in format 3: %s", e)

        return(body_text)

    def extractArticleBodyFormat4(self, docRoot):
        """ Extract Article Text content in format 4
        """
        body_text = ""
        try:
            # only get contents if its a blog type of article:
            body_root = docRoot.find_all("article", attrs={"class": "artData clr "})

            if len(body_root) > 0:
                firstTag = body_root[0]
                body_text = firstTag.getText()
                logger.debug("Successfully extracted article content in format 4")

        except Exception as e:
            logger.error("When extracting article content in format 4: %s", e)
        return(body_text)

    def extractArticleBodyFormat5(self, docRoot):
        """ Extract Article Text content in format 5
        # <div data-brcount = "10" class = "artText">
        """
        body_text = ""
        try:
            # only get contents if its a blog type of article:
            body_root = docRoot.find_all("div", attrs={"class": "artText"})

            if len(body_root) > 0:
                firstTag = body_root[0]
                body_text = firstTag.getText()
                logger.debug("Successfully extracted article content in format 5")

        except Exception as e:
            logger.error("When extracting article content in format 5: %s", e)
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
