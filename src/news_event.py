#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
# File name: news_event.py                                                                                #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: News Event class that stores the article and data supporting the web scraper                   #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
# Provides:                                                                                               #
#    NewsEvent                                                                                          #
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
from datetime import datetime
import json
import queue
from json import JSONEncoder
import bz2
import base64
import re

# import internal libraries
from scraper_utils import deDupeList, fixSentenceGaps


##########

# setup logging
logger = logging.getLogger(__name__)

##########


class NewsEvent(JSONEncoder):
    """ This object encapsulates News event data attributes and common functions applied to these
    """
    urlData = dict()
    triggerWordFlags = dict()
    uniqueID = ""
    html = None

    def getPublishDate(self):
        return(self.urlData["pubdate"])

    def getURL(self):
        return(self.urlData["URL"])

    def getModuleName(self):
        """ get the name of the module that generated this news item"""
        return(self.urlData["module"])

    def getHTML(self):
        return(self.html)

    def getBase64FromHTML(articleHTMLText):
        """ Get Base64 text data From HTML text
        """
        htmlBase64 = ""
        try:
            encodedBytes = base64.b64encode(articleHTMLText.encode("utf-8"))
            htmlBase64 = str(encodedBytes, "ascii")

        except Exception as e:
            logger.error("Error setting html content: %s", e)

        return(htmlBase64)

    def getHTMLFromBase64(htmlBase64):
        """ get HTML text From Base64 data
        """
        decoded_bytes = ""
        try:
            base64_bytes = htmlBase64.encode('ascii')
            decoded_bytes = base64.b64decode(base64_bytes)
        except Exception as e:
            logger.error("Error setting html content: %s", e)
        return(decoded_bytes.decode('utf-8'))

    def getText(self):
        textContent = ""
        if "text" in self.urlData.keys():
            textContent = self.urlData["text"]
        else:
            logger.error("Article does not have any text field")
        return(textContent)

    def getTextSize(self):
        textSize = 0
        try:
            textSize = len(self.getText())
        except Exception as e:
            logger.error("Error getting text size of article: %s", e)
        return(textSize)

    def getHTMLSize(self):
        htmlSize = 0
        try:
            htmlSize = len(self.html)
        except Exception as e:
            logger.error("Error getting html size of article: %s", e)
        return(htmlSize)

    def getAuthors(self):
        return(self.urlData["sourceName"])

    def getTriggerWords(self):
        return(self.triggerWordFlags)

    def getKeywords(self):
        return(self.urlData["keywords"])

    def getArticleID(self):
        return(self.urlData["uniqueID"])

    def getTextEmbedding(self):
        return(self.nlpDoc)

    def getFileName(self):
        return(self.fileName)

    def setClassification(self, classificationObj):
        self.urlData['classification'] = classificationObj

    def setTextEmbedding(self, nlpDoc):
        self.nlpDoc = nlpDoc

    def setHTML(self, htmlContent):
        self.html = htmlContent

    def setFileName(self, fileName):
        self.fileName = fileName

    def setPublishDate(self, publishDate):
        """ set the Publish Date of article """
        try:
            self.urlData["pubdate"] = str(publishDate.strftime("%Y-%m-%d"))
        except Exception as e:
            logger.error("Error setting publish date of article: %s", e)
            self.urlData["pubdate"] = str(datetime.now().strftime("%Y-%m-%d"))

    def setModuleName(self, moduleName):
        """ Set the name of the module that generated this news item"""
        self.urlData["module"] = moduleName

    def setTriggerWordFlag(self, triggerKey, triggerFlag):
        """ Add trigger word flag value for given article"""
        self.triggerWordFlags[triggerKey] = triggerFlag

    def identifyTriggerWordFlags(self, configur):
        """ Identify Trigger Word Flags, read from config file """
        if 'triggerwords' in configur.sections():
            section = configur['triggerwords']
            if section.name == 'triggerwords':
                for key, item in section.items():
                    matchPat = re.compile(str(item).strip())
                    regMatchRes = matchPat.search(self.getText().lower())
                    if regMatchRes is not None:
                        self.setTriggerWordFlag(key, 1)
                    else:
                        self.setTriggerWordFlag(key, 0)
        self.urlData["triggerwords"] = self.triggerWordFlags

    def setTitle(self, articleTitle):
        """ Set the title """
        self.urlData["title"] = str(articleTitle)

    def setKeyWords(self, articleKeyWordsList):
        """ set the keywords in the article
        """
        resultList = []
        try:
            for keyword in articleKeyWordsList:
                # clean words, trim whitespace:
                resultList.append(NewsEvent.cleanText(keyword))
            # de-duplicate the list
            resultList = deDupeList(resultList)
        except Exception as e:
            logger.error("Error cleaning keywords for article: %s", e)
        self.urlData["keywords"] = resultList

    def setText(self, articleText):
        self.urlData["text"] = NewsEvent.cleanText(articleText)

    def setIndustries(self, articleIndustryList):
        self.urlData["industries"] = articleIndustryList

    def setCategory(self, articleCategory):
        self.urlData["category"] = str(articleCategory)

    def setURL(self, sURL):
        self.urlData["URL"] = str(sURL)

    def setArticleID(self, uniqueID):
        self.uniqueID = uniqueID
        self.urlData["uniqueID"] = str(uniqueID)

    def setSource(self, sourceName):
        self.urlData["sourceName"] = str(sourceName)

    def toJSON(self):
        """ Converts python object into json.
        See reference page at - https://docs.python.org/3/library/json.html
        """
        return json.dumps(self.urlData)

    def readFromJSON(self, jsonFileName):
        """ read from JSON file into object
        """
        # logger.debug("Reading JSON file to load previously saved article %s", jsonFileName)
        try:
            with open(jsonFileName, 'r', encoding='utf-8') as fp:
                self.urlData = json.load(fp)
                fp.close()
        except Exception as theError:
            logger.error("Exception caught reading JSON file %s: %s", jsonFileName, theError)

    def cleanText(textInput):
        """ Clean text - replace unicode characters, fix space gaps, remove repeated characters, etc.

        :parameter textInput: The text string to clean
        :type textInput: str
        :return: the cleaned text string
        :rtype: str
        """
        cleanText = textInput
        if textInput is not None and len(textInput) > 1:
            try:
                # replace special characters:
                replaceWithSpaces = ['\u0915', '\u092f', '\u0938', '\u091a', '\u0941', '\u093e', '\u0906',
                                     '\u092c', '\u093e', '\u0902', '\u0917', '\u0925', '\u092e', '\u0923',
                                     '\u0930', '\u0908', '\u0926', '\u0932', '\u0905', '\u092d', '\u0924',
                                     '\u0938', '\u092a', '\u0924', '\u0909', '\u091c', '\u094b', '\u0940',
                                     'â€™', '🙂', "\u200b", 'â', '™', "\U0001f642", "\x93", "\x94"]
                for charToReplace in replaceWithSpaces:
                    cleanText = cleanText.replace(charToReplace, " ")
                # replace specific characters with alternates
                cleanText = cleanText.replace(" Addl. ", " Additional ")
                cleanText = cleanText.replace(" M/s.", " Messers")
                cleanText = cleanText.replace(" m/s.", " Messers")
                cleanText = cleanText.replace(' Rs.', ' Rupees ')
                cleanText = cleanText.replace('₹', ' Rupees ')
                cleanText = cleanText.replace('$', ' Dollars ')
                cleanText = cleanText.replace('€', " Euros ")
                cleanText = cleanText.replace("\t", " ")
                cleanText = cleanText.replace('—', "-")
                cleanText = cleanText.replace("\u2014", "-")
                cleanText = cleanText.replace('–', "-")
                cleanText = cleanText.replace("\u2013", "-")
                cleanText = cleanText.replace('’', "'")
                cleanText = cleanText.replace("\u2019", "'")
                cleanText = cleanText.replace('‘', "'")
                cleanText = cleanText.replace("\u2018", "'")
                cleanText = cleanText.replace('”', "'")
                cleanText = cleanText.replace("\u201d", "'")
                cleanText = cleanText.replace('“', "'")
                cleanText = cleanText.replace("\u201c", "'")
                cleanText = cleanText.replace('​', "'")  # yes, there is a special character here.
                # remove non utf-8 characters
                cleanText = cleanText.encode('utf-8', errors="replace").decode('utf-8', errors='ignore').strip()
                cleanText = fixSentenceGaps(cleanText)
            except Exception as e:
                logger.error("Error cleaning text: %s", e)
        return(cleanText)

    def writeFiles(self, fileNameWithOutExt, htmlContent, saveHTMLFile=False):
        """ Write news event data urlData to JSON file.
         Optionally, saves HTML data into a corresponding html file.

         :param fileNameWithOutExt: Required filename to save the json data,
          it is the full path without the file extension.
         :type fileNameWithOutExt: str
         :param htmlContent: HTML content to be saved optionally, pass empty string if this is not required.
         :type fileNameWithOutExt: str
         :param saveHTMLFile: Flag that indicates whether the HTML content needs to be saved or not, default is False.
         :type saveHTMLFile: bool
        """
        fullHTMLPathName = ""
        fullPathName = ""
        try:
            parentDirName = os.path.dirname(fileNameWithOutExt)
            # first check if directory of given date exists, or not
            if os.path.isdir(parentDirName) is False:
                # dir does not exist, so try creating it:
                os.mkdir(parentDirName)
        except Exception as theError:
            logger.error("Exception when saving article creating parent directory %s: %s", parentDirName, theError)
        try:
            if saveHTMLFile is True:
                fullHTMLPathName = fileNameWithOutExt + ".html.bz2"
                with bz2.open(fullHTMLPathName, "wb") as fpt:
                    # Write compressed data to file
                    fpt.write(htmlContent.encode("utf-8"))
                    fpt.close()
        except Exception as theError:
            logger.error("Exception caught writing data to html file %s: %s", fullHTMLPathName, theError)
        try:
            jsonContent = self.toJSON()
            fullPathName = fileNameWithOutExt + ".json"
            with open(fullPathName, 'wt', encoding='utf-8') as fp:
                fp.write(jsonContent)
                fp.close()
                logger.debug('Saved article as json file: %s', fullPathName)
        except Exception as theError:
            logger.error("Exception caught saving data to json file %s: %s", fullPathName, theError)
            # throw the exception back to calling routines:
            raise theError

    def importNewspaperArticleData(self, newspaperArticle):
        """ Import Data from newspaper library's Article class
        """
        try:
            # check and get authors/sources
            if len(newspaperArticle.authors) > 0:
                self.setSource(newspaperArticle.authors[0])
            else:
                self.setSource("")
            # set publishDate as current date time if article's publish_date is null
            if newspaperArticle.publish_date is None or newspaperArticle.publish_date == '':
                self.setPublishDate(datetime.now())
            else:
                self.setPublishDate(newspaperArticle.publish_date)
            self.setText(newspaperArticle.text)
            self.setTitle(newspaperArticle.title)
            self.setURL(newspaperArticle.url)
            self.setHTML(newspaperArticle.html)
            allKeywords = []
            if type(newspaperArticle.keywords).__name__ == 'list':
                allKeywords = allKeywords + newspaperArticle.keywords
            if type(newspaperArticle.meta_data['keywords']).__name__ == 'str':
                allKeywords = allKeywords + newspaperArticle.meta_data['keywords'].split(',')
            if type(newspaperArticle.meta_data['news_keywords']).__name__ == 'str':
                allKeywords = allKeywords + newspaperArticle.meta_data['news_keywords'].split(',')
            self.setKeyWords(allKeywords)
        except Exception as theError:
            logger.error("Exception caught importing newspaper article: %s", theError)


# # end of file ##
