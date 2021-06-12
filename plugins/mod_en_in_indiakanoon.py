#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_en_in_indiakanoon.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: plugin for India Kanoon portal on legal rulings
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
import bs4

from base_plugin import basePlugin
from data_structs import Types
# from data_structs import ScrapeError
from scraper_utils import deDupeList, filterRepeatedchars

##########

logger = logging.getLogger(__name__)


class mod_en_in_indiakanoon(basePlugin):
    """ Web Scraping plugin - mod_en_in_indiakanoon
    Description: india kanoon portal for legal rulings
    Language: English
    Country: India
    """
    # define a minimum count of characters for text body, article content below this limit will be ignored
    minArticleLengthInChars = 400

    # implies web-scraper for news content, see data_structs.py for other types
    pluginType = Types.MODULE_NEWS_CONTENT

    # main webpage URL
    mainURL = "https://indiankanoon.org/"

    # RSS feeds to pick up latest news article links
    all_rss_feeds = ["https://indiankanoon.org/feeds/latest/judgments/",
                     "https://indiankanoon.org/feeds/latest/allahabad/",
                     "https://indiankanoon.org/feeds/latest/amravati/",
                     "https://indiankanoon.org/feeds/latest/bombay/",
                     "https://indiankanoon.org/feeds/latest/chattisgarh/",
                     "https://indiankanoon.org/feeds/latest/chennai/",
                     "https://indiankanoon.org/feeds/latest/delhi/",
                     "https://indiankanoon.org/feeds/latest/delhiorders/",
                     "https://indiankanoon.org/feeds/latest/gauhati/",
                     "https://indiankanoon.org/feeds/latest/gujarat/",
                     "https://indiankanoon.org/feeds/latest/himachal_pradesh/",
                     "https://indiankanoon.org/feeds/latest/jammu/",
                     "https://indiankanoon.org/feeds/latest/jharkhand/",
                     "https://indiankanoon.org/feeds/latest/jodhpur/",
                     "https://indiankanoon.org/feeds/latest/karnataka/",
                     "https://indiankanoon.org/feeds/latest/kerala/",
                     "https://indiankanoon.org/feeds/latest/kolkata/",
                     "https://indiankanoon.org/feeds/latest/kolkata_app/",
                     "https://indiankanoon.org/feeds/latest/madhyapradesh/",
                     "https://indiankanoon.org/feeds/latest/manipur/",
                     "https://indiankanoon.org/feeds/latest/meghalaya/",
                     "https://indiankanoon.org/feeds/latest/orissa/",
                     "https://indiankanoon.org/feeds/latest/patna/",
                     "https://indiankanoon.org/feeds/latest/patna_orders/",
                     "https://indiankanoon.org/feeds/latest/punjab/",
                     "https://indiankanoon.org/feeds/latest/rajasthan/",
                     "https://indiankanoon.org/feeds/latest/supremecourt/",
                     "https://indiankanoon.org/feeds/latest/scorders/",
                     "https://indiankanoon.org/feeds/latest/srinagar/",
                     "https://indiankanoon.org/feeds/latest/sikkim/",
                     "https://indiankanoon.org/feeds/latest/tripura/",
                     "https://indiankanoon.org/feeds/latest/telangana/",
                     "https://indiankanoon.org/feeds/latest/uttaranchal/",
                     "https://indiankanoon.org/feeds/latest/districtcourts/",
                     "https://indiankanoon.org/feeds/latest/delhidc/",
                     "https://indiankanoon.org/feeds/latest/bangaloredc/",
                     "https://indiankanoon.org/feeds/latest/tribunals/",
                     "https://indiankanoon.org/feeds/latest/greentribunal/",
                     "https://indiankanoon.org/feeds/latest/cci/",
                     "https://indiankanoon.org/feeds/latest/cic/",
                     "https://indiankanoon.org/feeds/latest/aptel/",
                     "https://indiankanoon.org/feeds/latest/tdsat/",
                     "https://indiankanoon.org/feeds/latest/sebisat/",
                     "https://indiankanoon.org/feeds/latest/cestat/",
                     "https://indiankanoon.org/feeds/latest/consumer_national/",
                     "https://indiankanoon.org/feeds/latest/consumer_state/",
                     "https://indiankanoon.org/feeds/latest/nclat/",
                     "https://indiankanoon.org/feeds/latest/cat/",
                     "https://indiankanoon.org/feeds/latest/itat/",
                     "https://indiankanoon.org/feeds/latest/others/"
                     ]

    # fetch only URLs containing the following substrings:
    validURLStringsToCheck = []

    # this list of URLs will be visited to get links for articles,
    # but their content will not be scraped to pick up news content
    nonContentURLs = [mainURL,
                      'https://indiankanoon.org/members/',
                      'https://indiankanoon.org/advanced.html',
                      'https://indiankanoon.org/disclaimer.html',
                      'https://indiankanoon.org/feeds/',
                      'https://api.indiankanoon.org',
                      'https://indiankanoon.org/members',
                      'https://indiankanoon.org/browse/',
                      'https://indiankanoon.org/search_tips.html',
                      'https://indiankanoon.org/court_case_online.html',
                      'https://indiankanoon.org/members/signup/',
                      'https://indiankanoon.org/members/passwdrst/'
                      ]

    nonContentStrings = []

    # never fetch URLs containing these strings:
    invalidURLSubStrings = ['https://indiankanoon.org/search/?']

    # https://indiankanoon.org/doc/131060183/
    urlUniqueRegexps = [
                        r'(https:\/\/)(indiankanoon.org\/doc\/)([0-9]{5,})(\/)',
                        r'(https:\/\/)(indiankanoon.org\/.+)([0-9]{5,})(\.html)'
                        ]

    articleDateRegexps = {r'(on )([0-9]+ [a-zA-Z]{3}, [0-9]{4})(<\/TITLE>)': '%d %b, %Y',
                          r'(on )([0-9]+ [a-zA-Z]{3,}, [0-9]{4})(<\/TITLE>)': '%d %B, %Y',
                          r'(Date: )([0-9]{2}\/[0-9]{2}\/20[0-9]{2})': '%d/%m/%Y',
                          r'(.)([0-9]{1,2} [January|February|March|April|May|June|July|August|September|October|November' +
                          r'|December]{3,}, [2|1][0-9]{2})': '%d %B, %Y',
                          r'(.)([0-9]{1,2}th DAY OF [January|February|March|April|May|June|July|August|September|October' +
                          r'|November|December]{3,}, [2|1][0-9]{2})': '%dth DAY OF %B, %Y',
                          r'(.)([0-9]{1,2}th [January|February|March|April|May|June|July|August|September|October|November' +
                          r'|December]{3,} [2|1][0-9]{2})': '%dth %B %Y',
                          r'(.)([0-9]{1,2}st [January|February|March|April|May|June|July|August|September|October|November' +
                          r'|December]{3,} [2|1][0-9]{2})': '%dst %B %Y',
                          r'(.)([0-9]{1,2}nd [January|February|March|April|May|June|July|August|September|October|November' +
                          r'|December]{3,} [2|1][0-9]{2})': '%dnd %B %Y'
                          # TODO: 11th April, 1944
                          }

    invalidTextStrings = ['Try out our Premium Member services']
    subStringsToFilter = ['<p>Try out our <b>Premium Member</b> services: <b>Virtual Legal Assistant</b>,  ' +
                          '<b>Query Alert Service</b> and an ad-free experience. <a href="/members/">' +
                          'Free for one month</a> and pay only if you like it.</p>']
    allowedDomains = ['indiankanoon.org']

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
        self.articleDateRegexps.update(super().articleDateRegexps)
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
        """ Extract the author/Source from the html content
        """
        authors = []
        try:
            if type(htmlText) == bytes:
                htmlText = htmlText.decode('UTF-8')
            authorPat = re.compile(r"(<div class=\"docsource_main\">)([a-zA-Z0-9 \-]{4,})(</div>)")
            matchRes = authorPat.search(htmlText)
            if matchRes is not None:
                authors.append(matchRes.group(2))
        except Exception as e:
            logger.error("Error extracting authors: %s", e)
        return(authors)

    def extractArticleBody(self, htmlContent):
        """ Extract the text body of this article
        """
        allText = []
        body_text = " "
        htmlContent = htmlContent.decode('UTF-8') if type(htmlContent) == bytes else htmlContent
        try:
            result_tag = BeautifulSoup(htmlContent, 'lxml').find_all("div", attrs={"class": "docsource_main"})
            if result_tag is not None:
                # FIXME: list index out of range - 'https://indiankanoon.org/browse/jodhpur/'
                for member in result_tag[0].parent.children:
                    if isinstance(member, bs4.NavigableString):
                        allText.append(str(member).replace("\n", " "))
                    elif type(member) == bs4.element.Tag and (
                            not (member.has_attr('class') and "ad_doc" in member.attrs['class']) and (
                                member is not None) and (len(member.contents) > 1)
                            ):
                        subItemText = "\n"
                        for subItem in member.contents:
                            if type(subItem) == bs4.element.Tag:
                                subItemText = subItemText + " \n " + subItem.text
                            elif subItem is not None:
                                subItemText = subItemText + " \n " + subItem.string
                        allText.append(subItemText)
                    else:
                        allText = allText + member.contents
            for item in allText:
                body_text = body_text + " " + str(item).strip()
        except Exception as e:
            logger.error("Error retrieving content of article: %s, URL = %s", e, self.URLToFetch)
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
