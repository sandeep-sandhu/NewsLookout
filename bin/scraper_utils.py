#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: scraper_utils.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Helper class with utility functions supporting the web scraper
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 Provides:
    retainValidArticles()
    removeInValidArticles()
    removeStartTrailQuotes()
    decodeSecret()
    saveObjToJSON()
    cutStrFromTag()
    cutStrBetweenTags()
    checkAndParseDate()
    getPreviousDaysDate()
    loadPlugins()
    getNetworkLocFromURL()
    extractLinks()
    normalizeURL()
    checkAndGetNLTKData()
    calculateCRC32()
    printAppVersion()

 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.

"""

##########

# import standard python libraries:
import sys
import os
import importlib
import importlib.resources
from datetime import date, datetime, timedelta

import json
import base64
import zlib
import logging

# import web retrieval and text processing python libraries:
import nltk
from tld import get_tld

from data_structs import NewsArticle, URLListHelper

# setup logging
logger = logging.getLogger(__name__)

##########


def retainValidArticles(articleList, validURLPatternsList):
    """ retain only valid URLs
    """
    valid_articles = []

    if len(validURLPatternsList) < 1:
        return(articleList)
    else:
        for article in articleList:

            # only keep following URLs which contain strings matching pattern
            for strCheck in validURLPatternsList:
                try:
                    if type(article).__name__ == 'Article':

                        if article.url.find(strCheck) > -1 and len(article.url) > 9:
                            # this article URL contains the valid article substring, hence, add it to the valid list
                            valid_articles.append(article.url)
                            break

                    elif type(article).__name__ == 'str':

                        if article.find(strCheck) > -1 and len(article) > 9:
                            valid_articles.append(article)
                            break

                except Exception as e:
                    logger.error("ERROR retaining valid article list: %s", e)

    logger.debug("Retaining valid articles: Count of valid articles remaining = %s", len(valid_articles))
    return(valid_articles)


def removeInValidArticles(articleList, invalidURLPatternsList):
    """ remove InValid Articles """

    valid_articles = []

    for article in articleList:

        try:
            # delete URLs which contain any of the strings matching pattern
            checkCondition = True

            for strCheck in invalidURLPatternsList:

                if type(article).__name__ == 'Article':
                    checkCondition = checkCondition and (article.url.find(strCheck) == -1)

                elif type(article).__name__ == 'str':
                    checkCondition = checkCondition and (article.find(strCheck) == -1)

            if checkCondition is True:
                valid_articles.append(article)

        except Exception as e:
            logger.error("ERROR filtering out invalid article list: %s", e)

    return(valid_articles)


def removeStartTrailQuotes(textString):
    """ Remove starting and or trailing quotes from strings """
    resultString = ""

    resultString = textString.strip('\"').strip("'")

    return(resultString)


def decodeSecret(encodedText, keyValue):
    """ Decode Secret, use Base64 for now
    , ignore the keyValue
    """
    decodedText = ""

    try:
        base64_bytes = encodedText.encode('ascii')

        decoded_bytes = base64.b64decode(base64_bytes)

        decodedText = decoded_bytes.decode('utf-8')

    except Exception as e:
        logger.error("Error decoding secret: %s", e)

    return(decodedText)


def saveObjToJSON(self, jsonFileName, listToSave):
    """ Save the object to a JSON format file !!!!
    """
    try:
        jsonString = json.dumps(listToSave)
        with open(jsonFileName, 'wt', encoding='utf-8') as fp:
            fp.write(jsonString)
            fp.close()
    except Exception as e:
        logger.error("Exception caught saving object to JSON file: %s", e)


def cutStrFromTag(sourceStr, startTagStr):
    """ Cut part of the source String starting from substring from Tag till its end """
    resultStr = ""

    try:
        start_pos = sourceStr.find(startTagStr) + len(startTagStr)

        if start_pos > -1:
            resultStr = sourceStr[start_pos:]

    except Exception as e:
        logger.error("ERROR extracting string starting from tags: %s", e)

    return(resultStr)


def cutStrBetweenTags(sourceStr, startTagStr, endTagStr):
    """ Cut source string between given substring Tags """

    resultStr = ""

    try:
        start_pos = sourceStr.find(startTagStr) + len(startTagStr)

        if start_pos > -1:
            snipped_string = sourceStr[start_pos:]
            end_pos = snipped_string.find(endTagStr)

            if end_pos > -1:
                resultStr = snipped_string[:end_pos]

    except Exception as e:
        logger.error("ERROR extracting string between two tags: %s", e)

    return(resultStr)


def checkAndParseDate(dateStr):
    """ Check and Parse Date String, set it to todays date if its in future """

    logger.debug("Checking date string: %s", dateStr)

    try:
        runDate = datetime.strptime(dateStr, '%Y-%m-%d')

    except Exception as e:
        logger.error("Invalid date for retrieval (%s): %s; using todays date instead.",
                     dateStr, e)

    # get the current local date
    today = date.today()

    if runDate.date() > today:
        logger.error("Date for retrieval (%s) cannot be after today's date; using todays date instead.",
                     runDate.date())
        runDate = datetime.now()

    return(runDate)


def getPreviousDaysDate(runDate):
    """ Given a date input, get date object of previous day
    """
    businessDate = runDate - timedelta(days=1)
    return(businessDate)


def loadPlugins(configData):
    """load only enabled plugins from the class files in the package directory
    """

    pluginsDict = dict()

    # get list of plugins mentioned in the config file as comma separated list
    enabledPluginNames = removeStartTrailQuotes(configData['enabledPlugins']).split(',')

    pluginList = []
    for listItem in enabledPluginNames:
        pluginList.append(removeStartTrailQuotes(
             NewsArticle.cleanDirtyText(listItem)
             ))

    # de-dupe list:
    enabledPluginNames = URLListHelper.deDupeList(pluginList)

    plugins_dir = configData['plugins_dir']
    modulesPackageName = os.path.basename(plugins_dir)

    sys.path.append(configData['install_prefix'])
    sys.path.append(plugins_dir)

    for modfilename in importlib.resources.contents(modulesPackageName):

        # get full path:
        path = os.path.join(plugins_dir, modfilename)

        if os.path.isdir(path):  # skip directories
            continue

        # get only file name without file extension:
        modName = os.path.splitext(modfilename)[0]

        # only then load the module if module is enabled in the config file:
        if modName in enabledPluginNames:
            className = modName  # since class names of plugins are same as their module names
            try:
                logger.debug("Importing web-scraping plugin class: %s", modName)
                classObj = getattr(importlib.import_module(modName, package=modulesPackageName), className)
                pluginsDict[modName] = classObj()
            except Exception as e:
                logger.error("While importing plugin %s got exception: %s", modName, e)

    return pluginsDict


def getNetworkLocFromURL(URLStr):
    """ Derive network location from the given URL
    res.parsed_url contains the structure of the parsed URL
    """
    res = get_tld(URLStr, as_object=True)

    # SplitResult(scheme = 'https', netloc = 'auto.economictimes.indiatimes.com'
    # , path = '/news/auto-components/abcd', query = '', fragment = '')

    return(res.parsed_url.netloc)


def extractLinks(url, docRoot):
    """ Extract all Links from beautifulSoup document """
    allLinks = []
    section = docRoot.find_all("a")
    rootTLDObj = get_tld(url, as_object=True)

    if len(section) > 0:
        for tag in section:
            if tag.name == "a" and "href" in tag.attrs.keys():
                linkValue = tag['href']
                if linkValue.startswith('/'):
                    allLinks.append(
                                    rootTLDObj.parsed_url.scheme
                                    + '://'
                                    + rootTLDObj.parsed_url.netloc
                                    + linkValue)
                elif linkValue.startswith("javascript:") is False:
                    allLinks.append(linkValue)

    return(allLinks)


def normalizeURL(articleURL):
    """ Normalize the URL. Break into the structure:
     res.scheme = 'https'
     res.netloc = 'auto.economictimes.indiatimes.com'
     res.path = '/path1/sub-path2/'
     res.fragments = ''
     res.query = 'q = somesearch'
    """

    # change case to make it homogeneous
    normalisedURL = articleURL.lower()

    # res = get_tld(URLStr, as_object = True)

    # run path through url decoder to get common representation:

    # expand and resolve relative urls, for example: ../

    return(normalisedURL)


def checkAndGetNLTKData():
    """ Check if NLTK taggers and tokernzers are available.
    If not, then download the NLTK Data """
    try:
        fsPointer = nltk.data.find('tokenizers/punkt')
        logger.debug("NLTK punkt tokenizers is available.")
    except Exception as e:
        logger.debug("Error: %s", e)
        downloadResult = nltk.download('punkt')
        logger.debug("Download of punkt successful? %s", downloadResult)

    try:
        fsPointer = nltk.data.find('taggers/maxent_treebank_pos_tagger')
        logger.debug("NLTK maxent_treebank_pos_tagger is available.")
    except Exception as e:
        logger.debug("Error: %s", e)
        downloadResult = nltk.download('maxent_treebank_pos_tagger')
        logger.debug("Download of maxent_treebank_pos_tagger successful? %s", downloadResult)

    try:
        fsPointer = nltk.data.find('corpora/reuters.zip')
        logger.debug("NLTK reuters is available: %s", fsPointer)
    except Exception as e:
        logger.debug("Error: %s", e)
        downloadResult = nltk.download('reuters')
        logger.debug("Download of reuters successful? %s", downloadResult)

    try:
        fsPointer = nltk.data.find('corpora/universal_treebanks_v20.zip')
        logger.debug("NLTK universal_treebanks_v20 is available: %s", fsPointer)
    except Exception as e:
        logger.debug("Error: %s", e)
        downloadResult = nltk.download('universal_treebanks_v20')
        logger.debug("Download of universal_treebanks_v20 successful? %s", downloadResult)


def calculateCRC32(text):
    """ use zlib's CRC32 calculation
    """
    crc = zlib.crc32(text) % (2 ** 32)
    return(hex(crc))


def printAppVersion():
    listOfGlobals = globals()
    print("Application version = ",
          listOfGlobals['app_inst'].appQueue.configData['version'])

# # end of file ##
