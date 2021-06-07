#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_moneycontrol.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for Money Control
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

from base_plugin import basePlugin
from data_structs import Types
# from data_structs import ScrapeError
from scraper_utils import deDupeList, filterRepeatedchars

##########

logger = logging.getLogger(__name__)


class mod_en_in_moneycontrol(basePlugin):
    """ Web Scraping plugin - mod_en_in_moneycontrol for MoneyControl news and market data portal
    """
    # The minimum count of characters required for text body, article content size below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    mainURL = 'https://www.moneycontrol.com/news/'

    all_rss_feeds = ["https://www.moneycontrol.com/rss/MCtopnews.xml",
                     "https://www.moneycontrol.com/rss/results.xml",
                     "https://www.moneycontrol.com/rss/marketreports.xml",
                     "https://www.moneycontrol.com/rss/economy.xml",
                     "https://www.moneycontrol.com/rss/business.xml",
                     "https://www.moneycontrol.com/rss/latestnews.xml"
                     ]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = ['https://www.moneycontrol.com/']

    # never fetch these URLs:
    invalidURLSubStrings = ['/news_html_files/pdffiles/',
                            'twitter.com/',
                            'facebook.com/',
                            'whatsapp.com/',
                            't.me/moneycontrolcom',
                            'plus.google.com/share',
                            'www.linkedin.com/share'
                            ]

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://www.moneycontrol.com/india/bestportfoliomanager/investment-tool',
                      'https://www.moneycontrol.com/news/mcminis/',
                      'https://www.moneycontrol.com/news/technology/',
                      'https://www.moneycontrol.com/news/technology/auto',
                      'https://www.moneycontrol.com/news/masterclass-webinar/',
                      'https://www.moneycontrol.com/news/opinion/',
                      'https://www.moneycontrol.com/webinar',
                      'https://www.moneycontrol.com/equity-research/',
                      'https://www.moneycontrol.com/news/photos/',
                      'https://www.moneycontrol.com/news/business/startups/',
                      'https://www.moneycontrol.com/news/business/',
                      'https://www.moneycontrol.com/news/business/markets/',
                      'https://www.moneycontrol.com/news/business/economy/',
                      'https://www.moneycontrol.com/news/business/stocks/',
                      'https://www.moneycontrol.com/news/india/',
                      'https://www.moneycontrol.com/news/world/',
                      'https://www.moneycontrol.com/news/business/companies',
                      'https://www.moneycontrol.com/news/trends/',
                      'https://www.moneycontrol.com/news/business/personal-finance/',
                      'https://www.moneycontrol.com/stocksmarketsindia/',
                      'https://www.moneycontrol.com/markets/indian-indices/',
                      'https://www.moneycontrol.com/stocks/marketstats/index.php',
                      'https://www.moneycontrol.com/stocks/marketstats/nsegainer/index.php',
                      'https://www.moneycontrol.com/stocks/marketstats/nseloser/index.php',
                      'https://www.moneycontrol.com/stocks/marketstats/onlybuyers.php',
                      'https://www.moneycontrol.com/stocks/marketstats/onlysellers.php',
                      'https://www.moneycontrol.com/stocks/marketstats/nsehigh/index.php',
                      'https://www.moneycontrol.com/stocks/marketstats/nselow/index.php',
                      'https://www.moneycontrol.com/stocks/marketstats/nse_pshockers/index.php',
                      'https://www.moneycontrol.com/stocks/marketstats/nse_vshockers/index.php',
                      'https://www.moneycontrol.com/stocks/marketstats/nsemact1/index.php',
                      'https://www.moneycontrol.com/markets/global-indices/',
                      'https://www.moneycontrol.com/india-investors-portfolio/',
                      'https://www.moneycontrol.com/markets/fno-market-snapshot',
                      'https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php',
                      'https://www.moneycontrol.com/stocks/marketinfo/upcoming_actions/home.html',
                      'https://www.moneycontrol.com/markets/earnings/',
                      'https://www.moneycontrol.com/mccode/currencies/',
                      'https://www.moneycontrol.com/commodity/',
                      'https://www.moneycontrol.com/ipo/',
                      'https://www.moneycontrol.com/markets/premarket/',
                      'https://www.moneycontrol.com/markets/stock-advice/',
                      'https://www.moneycontrol.com/broker-research/',
                      'https://www.moneycontrol.com/markets/technicals/',
                      'https://www.moneycontrol.com/pro-interviews',
                      'https://www.moneycontrol.com/fixed-income/bonds/',
                      'https://www.moneycontrol.com/cryptocurrency/',
                      'https://www.moneycontrol.com/mccode/tools/',
                      'https://www.moneycontrol.com/news/tags/companies.html',
                      'https://www.moneycontrol.com/news/business/mutual-funds/',
                      'https://www.moneycontrol.com/news/business/ipo/',
                      'https://www.moneycontrol.com/real-estate-property/',
                      'https://www.moneycontrol.com/news/tags/technical-analysis.html',
                      'https://www.moneycontrol.com/news/business/commodity/',
                      'https://www.moneycontrol.com/news/tags/currency.html',
                      'https://www.moneycontrol.com/news/news-all/',
                      'https://www.moneycontrol.com/news/fintech/',
                      'https://www.moneycontrol.com/podcast/',
                      'https://www.moneycontrol.com/news/tags/slideshows.html',
                      'https://www.moneycontrol.com/news/infographic/',
                      'https://www.moneycontrol.com/news/videos/',
                      'https://www.moneycontrol.com/news/politics/',
                      'https://www.moneycontrol.com/news/cricket/',
                      'https://www.moneycontrol.com/news/entertainment/',
                      'https://www.moneycontrol.com/news/tags/travel.html',
                      'https://www.moneycontrol.com/news/tags/lifestyle.html',
                      'https://www.moneycontrol.com/news/tags/health-and-fitness.html',
                      'https://www.moneycontrol.com/news/tags/education.html',
                      'https://www.moneycontrol.com/news/tags/science.html',
                      'https://www.moneycontrol.com/news/tags/books.html',
                      'https://www.moneycontrol.com/commodities/mcx/gainers/all_0.html',
                      'https://www.moneycontrol.com/commodities/mcx/losers/all_0.html',
                      'https://www.moneycontrol.com/commodities/mcx/active_value/all_0.html',
                      'https://www.moneycontrol.com/commodities/mcx/most_active/all_0.html',
                      'https://www.moneycontrol.com/shows/commodities-moneycontrol/',
                      'https://www.moneycontrol.com/mutualfundindia/',
                      'https://www.moneycontrol.com/mutual-funds/find-fund/',
                      'https://www.moneycontrol.com/mutual-funds/best-funds/equity.html',
                      'https://www.moneycontrol.com/mutual-funds/performance-tracker/returns/large-cap-fund.html',
                      'https://www.moneycontrol.com/mutual-funds/performance-tracker/sip-returns/large-cap-fund.html',
                      'https://www.moneycontrol.com/mf/etf/',
                      'https://www.moneycontrol.com/mutual-funds/new-fund-offers',
                      'https://www.moneycontrol.com/mutual-funds/performance-tracker/all-categories',
                      'https://www.moneycontrol.com/mutualfundindia/learn/',
                      'https://www.moneycontrol.com/mf/returns-calculator.php',
                      'https://www.moneycontrol.com/planning_desk/assetallocater.php',
                      'https://www.moneycontrol.com/mf/sipcalculator.php',
                      'https://www.moneycontrol.com/mutualfundindia/mutual-fund-discussion-forum/',
                      'https://www.moneycontrol.com/mutual-funds/portfolio-management/PORTFOLIO/returns',
                      'https://www.moneycontrol.com/mutual-funds/portfolio-management/WATCHLIST/returns',
                      'https://www.moneycontrol.com/personal-finance/',
                      'https://www.moneycontrol.com/personal-finance/investing/',
                      'https://www.moneycontrol.com/personal-finance/insurance/',
                      'https://www.moneycontrol.com/personal-finance/banking/',
                      'https://www.moneycontrol.com/personal-finance/financialplanning/',
                      'https://www.moneycontrol.com/personal-finance/tools/',
                      'https://www.moneycontrol.com/personal-finance/videos/',
                      'https://www.moneycontrol.com/forum-topics/ask-the-expert/',
                      'https://www.moneycontrol.com/personal-finance/explainer/',
                      'https://www.moneycontrol.com/personal-finance/income-tax-filing',
                      'https://www.moneycontrol.com/personal-finance/nps-national-pension-scheme',
                      'https://www.moneycontrol.com/fixed-income/calculator/fixed-deposit-calculator.html',
                      'https://www.moneycontrol.com/news/tags/company-fixed-deposits.html',
                      'https://www.moneycontrol.com/personal-finance/income-tax-filing/',
                      'https://www.moneycontrol.com/personal-finance/tools/quick-incometax-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/emergency-fund-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/loans',
                      'https://www.moneycontrol.com/personal-finance/tools/carloan-emi-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/emi-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/education-loan-emi-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/credit-card-debt-payoff-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/provident-fund-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/asset-allocation-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/debt-reduction-plan-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/debt-evaluation-calculator.html',
                      'https://www.moneycontrol.com/personal-finance/tools/current-expenses-calculator.html',
                      'https://www.moneycontrol.com/video-shows/',
                      'https://www.moneycontrol.com/shows/coffee-can-investing/',
                      'https://www.moneycontrol.com/shows/ideas-for-profit/',
                      'https://www.moneycontrol.com/shows/in-focus-with-udayan-mukherjee/',
                      'https://www.moneycontrol.com/shows/3-point-analysis/',
                      'https://www.moneycontrol.com/shows/technical-views/',
                      'https://www.moneycontrol.com/shows/reporters-take/',
                      'https://www.moneycontrol.com/shows/explained/',
                      'https://www.moneycontrol.com/shows/political-bazaar/',
                      'https://www.moneycontrol.com/shows/editors-take/',
                      'https://www.moneycontrol.com/shows/millennial-pulse/',
                      'https://www.moneycontrol.com/shows/modi-government-report-card/',
                      'https://www.moneycontrol.com/news/tags/podcast.html',
                      'https://www.moneycontrol.com/shows/the-market-podcast/',
                      'https://www.moneycontrol.com/shows/future-wise/',
                      'https://www.moneycontrol.com/shows/simply-save/',
                      'https://www.moneycontrol.com/shows/stock-picks-of-the-day/',
                      'https://www.moneycontrol.com/shows/coronavirus-essential/',
                      'https://www.moneycontrol.com/subscription/',
                      'https://www.moneycontrol.com/gamechangers/ambareeshbaliga',
                      'https://www.moneycontrol.com/gamechangers/cknarayan',
                      'https://www.moneycontrol.com/gamechangers/prashant-shah',
                      'https://www.moneycontrol.com/gamechangers/?mc_source=MC_Eng_DT_Sudarshan_Sukhani_Subscription',
                      'https://www.moneycontrol.com/gamechangers/tgnanasekar',
                      'https://www.moneycontrol.com/gamechangers/mecklai',
                      'https://www.moneycontrol.com/gamechangers/shubham-agarwal',
                      'https://www.moneycontrol.com/promos/pro.php',
                      'https://www.moneycontrol.com/stock-reports',
                      'https://www.moneycontrol.com/news/news-all.html',
                      'https://www.moneycontrol.com/ms/poker/',
                      'https://www.moneycontrol.com/msite/kotakmf-investmentor/',
                      'https://www.moneycontrol.com/msite/pharma-industry-conclave',
                      'https://www.moneycontrol.com/msite/rockwell-manufacturing-powering-economic-recovery/',
                      'https://www.moneycontrol.com/msite/commodity-ki-paathshala',
                      'https://www.moneycontrol.com/msite/rockwell-automation-make-in-india/?reg_form=true',
                      'https://www.moneycontrol.com/msite/unlocking-opportunities-in-metal-mining',
                      'https://www.moneycontrol.com/sanjeevani',
                      'https://www.moneycontrol.com/portfolio-management/user/update_profile',
                      'https://www.moneycontrol.com/bestportfolio/wealth-management-tool/stock_watchlist',
                      'https://www.moneycontrol.com/alerts/manage-alerts.php',
                      'https://www.moneycontrol.com/news/tags/coronavirus.html',
                      'https://www.moneycontrol.com/news/mgmtinterviews/chats/detail_new.php?type=upcoming',
                      'https://www.moneycontrol.com/news/mgmtinterviews/chats/archives_new.php',
                      'https://www.moneycontrol.com/news/mgmtinterviews/chats/detail_new.php',
                      'https://www.moneycontrol.com/ms/earth-360/?mc_source=MC&mc_medium=Trending&mc_campaign=Earth360',
                      'https://www.moneycontrol.com/msite/globalinvesting',
                      'https://www.moneycontrol.com/news/tags/mf-experts.html',
                      'https://www.moneycontrol.com/markets/indian-indices/?classic=true',
                      'https://www.moneycontrol.com/news/personal-finance/epfo/',
                      'https://www.moneycontrol.com/news/mcminis',
                      'https://www.moneycontrol.com/mutualfundindia/mutual-fund-discussion-forum',
                      'https://www.moneycontrol.com/video-shows/?classic=true',
                      'https://www.moneycontrol.com/podcast/?classic=true',
                      'https://www.moneycontrol.com/stock-premier-league/?classic=true',
                      'https://www.moneycontrol.com/subscription',
                      'https://www.moneycontrol.com/news/business/earnings/',
                      'https://www.moneycontrol.com/news/tags/amit-bakshi.html',
                      'https://www.moneycontrol.com/news/tags/eris-lifesciences.html',
                      'https://www.moneycontrol.com/news/tags/q4-profit.html',
                      'https://www.moneycontrol.com/news/tags/results.html',
                      'https://www.moneycontrol.com/india/newsarticle/rssfeeds/rssfeeds.php',
                      'https://www.moneycontrol.com/portfolio_demo/stock_watchlist.php',
                      'https://www.moneycontrol.com/tv/',
                      'https://www.moneycontrol.com/fixed-income/',
                      'https://www.moneycontrol.com/budget-2021/',
                      'https://www.moneycontrol.com/sensex/bse/sensex-live',
                      'https://www.moneycontrol.com/news/business',
                      'https://www.moneycontrol.com/news/business/markets',
                      'https://www.moneycontrol.com/news/business/stocks',
                      'https://www.moneycontrol.com/news/business/economy',
                      'https://www.moneycontrol.com/news/business/mutual-funds',
                      'https://www.moneycontrol.com/news/business/personal-finance',
                      'https://www.moneycontrol.com/news/business/ipo',
                      'https://www.moneycontrol.com/news/business/startups',
                      'https://www.moneycontrol.com/personal-finance/tools/retirement-planning-calculator.html',
                      'https://www.moneycontrol.com/mf/sipplanner.php',
                      'https://www.moneycontrol.com/stocks/sectors/banks-public-sector.html',
                      'https://www.moneycontrol.com/fixed-income/small-savings-schemes/',
                      'https://www.moneycontrol.com/fixed-income/bonds/listed-bonds/',
                      'https://www.moneycontrol.com/news/tags/pr-post.html',
                      'https://www.moneycontrol.com/gamechangers/',
                      'https://www.moneycontrol.com/travelcafe/',
                      'https://www.moneycontrol.com/gestepahead/',
                      'https://www.moneycontrol.com/sme/',
                      'https://www.moneycontrol.com/cdata/aboutus.php',
                      'https://www.moneycontrol.com/cdata/contact.php',
                      'https://www.moneycontrol.com/advertise-on-moneycontrol.html',
                      'https://www.moneycontrol.com/cdata/disclaim.php',
                      'https://www.moneycontrol.com/cdata/privacypolicy.php',
                      'https://www.moneycontrol.com/cdata/gdpr_cookiepolicy.php',
                      'https://www.moneycontrol.com/cdata/termsofuse.php',
                      'https://www.moneycontrol.com/career/',
                      'https://www.moneycontrol.com/glossary/',
                      'https://www.moneycontrol.com/faqs/',
                      'https://www.moneycontrol.com/news/sitemap/sitemap.php',
                      'https://www.moneycontrol.com/news/loans-294.html',
                      'https://www.moneycontrol.com/news/tags/planning.html',
                      'https://www.moneycontrol.com/news/tags/credit-cards.html',
                      'https://www.moneycontrol.com/news/tags/retirement.html',
                      'https://www.moneycontrol.com/live-market/ftse',
                      'https://www.moneycontrol.com/news/photos/'
                      'https://www.moneycontrol.com/indian-indices/cnx-nifty-9.html'
                      'https://www.moneycontrol.com//www.moneycontrol.com/markets/global-indices/'
                      'https://www.moneycontrol.com/live-index/nasdaq'
                      'https://www.moneycontrol.com//www.moneycontrol.com/commodity/',
                      'https://www.moneycontrol.com/stocks/marketstats/top100.php',
                      'https://www.moneycontrol.com/news/tags/commodities.html',
                      'https://www.moneycontrol.com/stocks/marketinfo/dividends_declared/index.php',
                      'https://www.moneycontrol.com/news/opinion.html',
                      'https://www.moneycontrol.com//www.moneycontrol.com/markets/action-in-markets.html',
                      'https://www.moneycontrol.com/news/interview/',
                      'https://www.moneycontrol.com/mutualfundindia/tools/',
                      'https://www.moneycontrol.com/news/tags/economy.html',
                      'https://www.moneycontrol.com/news/tags/business.html',
                      'https://www.moneycontrol.com/live-index/nasdaq',
                      'https://www.moneycontrol.com/news/tags/real-estate.html'
                      ]

    nonContentStrings = ['www.moneycontrol.com/stocksmarketsindia/?',
                         'www.moneycontrol.com/currency/bse-usdinr-price.html',
                         'www.moneycontrol.com/cdata/contact.php',
                         'www.moneycontrol.com/master-your-money/?',
                         'www.moneycontrol.com/india/mutualfunds/mutualfundsinfo/snapshot/',
                         'www.moneycontrol.com/india/stockpricequote/',
                         'www.moneycontrol.com/mutual-funds/performance-tracker/all-categories?',
                         'www.moneycontrol.com/news/eye-on-india/videos/',
                         'www.moneycontrol.com/commodity/gold-price.html',
                         'www.moneycontrol.com/commodity/silver-price.html',
                         'www.moneycontrol.com/commodity/crudeoil-price.html',
                         'www.moneycontrol.com/commodity/naturalgas-price.html',
                         'www.moneycontrol.com/commodity/ncdex/rmseed-price.html',
                         'www.moneycontrol.com/commodity/ncdex/sybeanidr-price.html',
                         'www.moneycontrol.com/commodity/ncdex/cocudakl-price.html',
                         'www.moneycontrol.com/gamechangers/marketsmith-india?',
                         'www.moneycontrol.com/gamechangers/swingtrader-india?',
                         'www.moneycontrol.com/msite/decoding-the-world-of-etf/?'
                         'www.moneycontrol.com/mcplus/portfolio/logout.php?',
                         'www.moneycontrol.com/msite/hdfc-life-insurance-plans?'
                         ]

    urlUniqueRegexps = [r"(^https\:\/\/www.moneycontrol.com\/.+)(_)([0-9]{6,})(\.html$)",
                        r"(https\:\/\/www.moneycontrol.com\/.+)(_)([0-9]{6,})(\.html)"]

    invalidTextStrings = []
    subStringsToFilter = []

    articleDateRegexps = {
        r'(<input type=\"hidden\" id=\"to_timestamp\" value=\")(20[0-9]{2}[0-9]{2}[0-9]{2}[0-9]{2}[0-9]{2}[0-9]{2})(\">)':
        '%Y%m%d%H%M%S'  # <div class="date_time">Dec 29, 04:12</div>
        }
    authorRegexps = []
    dateMatchPatterns = dict()
    urlMatchPatterns = []
    authorMatchPatterns = []

    allowedDomains = ["moneycontrol.com"]
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

    def extractIndustries(self, uRLtoFetch, htmlText):
        """ Extract the industry of the articles from its URL or contents
        """
        industries = []
        if type(htmlText) == bytes:
            htmlText = htmlText.decode('UTF-8')
        try:
            logger.debug("Extracting industries identified by the article.")
            # <div class="market_element">personal-finance</div>
            industryPattern = re.compile(r'(<div class=\"market_element\">)([A-Za-z0-9\-_\. ]{3,})(<\/div>)')
            matchRes = industryPattern.search(htmlText)
            if matchRes is not None:
                industries.append(matchRes.group(2))
        except Exception as e:
            logger.error("Error extracting industries: %s", e)
        return(industries)

    def extractAuthors(self, htmlText):
        """ Extract Authors/Agency/Source from html
        """
        authors = []
        if type(htmlText) == bytes:
            htmlText = htmlText.decode('UTF-8')
        try:
            authPattern = re.compile(r'(\"author\": \")([a-zA-Z0-9 \-\._]{3,})(\")')
            matchRes = authPattern.search(htmlText)
            if matchRes is not None:
                authors.append(matchRes.group(2))
        except Exception as e:
            logger.error("Error extracting news agent from text: %s", e)
        return(authors)

    def extractArticleBody(self, htmlContent):
        """ Extract article's text
        """
        try:
            # get article text data by parsing specific tags:
            articleText = ""
            tempArray = []
            try:
                docRoot = BeautifulSoup(htmlContent, 'lxml')
                articleTags = docRoot.find_all('div', attrs={'class': 'text_block'})
                for tag in articleTags:
                    tempArray = tempArray + (tag.find_all('p', text=True))
                for paraGraph in tempArray:
                    articleText = articleText + paraGraph.get_text()
            except Exception as e:
                logger.error("Error extracting article content: %s", e)
            except Warning as w:
                logger.warning("Warning when extracting article body: %s", w)
        except Exception as e:
            logger.error("Exception extracting article body: %s", e)
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
