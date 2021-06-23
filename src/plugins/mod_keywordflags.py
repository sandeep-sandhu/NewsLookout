#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
#                                                                                                         #
# File name: mod_keywordflags.py                                                                          #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-05-01                                                                                        #
# Purpose: Plugin for keyword based generation of indicator variables from news events                    #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
#                                                                                                         #
# Notice:                                                                                                 #
# This software is intended for demonstration and educational purposes only. This software is             #
# experimental and a work in progress. Under no circumstances should these files be used in               #
# relation to any critical system(s). Use of these files is at your own risk.                             #
#                                                                                                         #
# Before using it for web scraping any website, always consult that website's terms of use.               #
# Do not use this software to fetch any data from any website that has forbidden use of web               #
# scraping or similar mechanisms, or violates its terms of use in any other way. The author is            #
# not liable for such kind of inappropriate use of this software.                                         #
#                                                                                                         #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,                     #
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR                #
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE               #
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR                    #
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER                  #
# DEALINGS IN THE SOFTWARE.                                                                               #
#                                                                                                         #
# #########################################################################################################

# import standard python libraries:
import logging
import os
import re
import time
from tqdm import tqdm
from datetime import datetime

# import this project's python libraries:
from base_plugin import BasePlugin
from data_structs import Types
from news_event import NewsEvent

##########

logger = logging.getLogger(__name__)

###########


class mod_keywordflags(BasePlugin):
    """ Web Scraping plugin: mod_keywordflags
    For generating indicator variables based on keywords matched in the downloaded data
    """
    minArticleLengthInChars = 400
    pluginType = Types.MODULE_DATA_PROCESSOR  # implies data post-processor
    listOfFiles = []

    keyword_flag_regex = {
        'FLAG_EVENT_STRIKE': r'((labo[u]*r|employee.+)[ \-]+(strike|unrest|protest|.+disrupt|.+stay away)|strike.+violen|violen.+strike|(man.*power|union).+problem|recalcitrant labo[u]*r|accident.+fine|union.+petition)',
        'FLAG_EVENT_LAYOFFS': r'(employee (attrition|problem)|attrition.+employee|lay[ \-]*off)',
        'FLAG_EVENT_DIR_CHNG': r'(director.+[ \-]*appoint|appoint.+director|change of director|director change)',
        'FLAG_EVENT_FINE': r'(fined|fine of|impose.+fine|(fine|penalty).+impose)',
        'FLAG_EVENT_RESIGN': r'(director.+(quit|resign|step.+down|withdraw|retire|stand aside|bow out|cessation)|withdraw.+director)',
        'FLAG_EVENT_CUSTSATISFY': r'(customer.+dis[\-]*satisf)',
        'FLAG_EVENT_REGULATION': r'(regulatory (impact|change)|impact of regula|duty.+(hike|increase)|(hike|increase).+(duty|tax))',
        'FLAG_EVENT_OBSOLETE': r'(obsolete product|obsolescence|outdate.+product|product.+(outdate|obsolete))',
        'FLAG_EVENT_FRAUD': r'(fraud|funds.+(diver|siphon)|suspect.+fraud|swindl|(diver|siphon).+fund|fraud.+audit|money launder|red flag.+acc|audit.+suspect|forensic audit)',
        'FLAG_EVENT_TAXRAID': r'((notice|raid|penalty|sanction|puni|forfeit|trial|sentence).+(authorit|government|court|regulat)|(authorit|government|court).+(sanction|puni|penalty|fine|))',
        'FLAG_EVENT_LATEPAY': r'(delay.+(payment|dues|statutary)|(pay.+statutary))',
        'FLAG_EVENT_CUSTLOSS': r'(los[set].+customer|customer.+cancel)',
        'FLAG_EVENT_NONCORE': r'(business.+expan.+[non\- core]{1,}|diversif.+non[\- ]core|incorporat|acqui[resition]{2,}|merge|joint venture|organic grow|key opportunit|(additional|unforeseen) cost|recall|ban order|diversif.+business)',
        'FLAG_EVENT_DISPUTE': r'(promoter.+(dispute|disagree|feud|conflict)|(dispute|conflict|feud).+(promoter|management|director|partner)|infight)',
        'FLAG_EVENT_PRODREJECT': r'(consignment.+reject|product.+reject|brand value.+(diminish|low|reduc)|withdraw.+(product|good))',
        'FLAG_EVENT_AUDITRESIGN': r'((replace|change|in place of).+audit|auditor.+(change))',
        'FLAG_EVENT_SPONSWTHDR': r'(funds.+(decline|withdraw)|(not have|no longer|has no).+funds|disinvest|withdraw.+from.+project|delay [ofin]{2}.+subsidy|subsidy.+stop)',
        'FLAG_EVENT_YOYCAPDEC': r'(low capacity utili|capacity expansion.+(hold|stop))',
        'FLAG_EVENT_UTILDISRUPT': r'((water|electric|power|utility).+(violation|disrupt|los[st])|pollution control|los[st].+(water|electric|power|utility))',
        'FLAG_EVENT_PROMOTPERSLOAN': r'(promoter|director|founder|partner).+(personal loan)'
    }

    def __init__(self):
        """ Initialize the object
        """
        super().__init__()

    def additionalConfig(self, sessionHistoryObj):
        """ Perform additional configuration that is specific to this plugin.

        :param sessionHistoryObj: The session history object to be used by this plugin
         for putting items into the data processing competed queue.
        :return:
        """
        self.workDir = self.app_config.data_dir
        self.sessionHistDB = sessionHistoryObj

    def processDataObj(self, newsEventObj):
        """ Process given data object by this plugin.

        :param newsEventObj: The NewsEvent object to be processed.
        :type dataObj: NewsEvent
        """
        runDate = datetime.strptime(newsEventObj.getPublishDate(), '%Y-%m-%d')
        logger.info("Started keyword based flag derivation for news event: %s for date: %s",
                    newsEventObj.getFileName(), runDate.strftime('%Y-%m-%d'))
        self.identifyTriggerWordFlags(newsEventObj)
        # prepare filename:
        fileNameWOExt = newsEventObj.getFileName().replace('.json','')
        # save document to file:
        newsEventObj.writeFiles(fileNameWOExt, '', saveHTMLFile=False)

    def identifyTriggerWordFlags(self, documentObj):
        """ Identify Trigger Word Flags, read from config file """
        for keywordFlag in self.keyword_flag_regex.keys():
            matchPat = re.compile(str(self.keyword_flag_regex[keywordFlag]).strip())
            regMatchRes = matchPat.search(documentObj.getText().lower())
            if regMatchRes is not None:
                documentObj.setTriggerWordFlag(keywordFlag, 1)
            else:
                documentObj.setTriggerWordFlag(keywordFlag, 0)
        documentObj.urlData["triggerwords"] = documentObj.triggerWordFlags

# # end of file ##
