#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: scraper_utils.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Helper class with utility functions supporting the web scraper
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 Provides:
    retainValidArticles()
    removeInValidArticles()
    removeStartTrailQuotes()
    decodeSecret()
    saveObjToJSON()
    checkAndSanitizeConfigString()
    checkAndSanitizeConfigInt()
    deDupeList(listWithDuplicates)
    spaceGapAfterDot(matchobj)
    fixSentenceGaps(inputText)
    filterRepeatedchars()
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
import sys
import os
import importlib
import importlib.resources
from datetime import date, datetime, timedelta
from collections import OrderedDict
import re
import json
import base64
import zlib
import logging

# import web retrieval and text processing python libraries:
import nltk
from tld import get_tld


# setup logging
logger = logging.getLogger(__name__)

##########


def retainValidArticles(articleList, validURLPatternsList):
    """ Retain only valid URLs
    """
    valid_articles = []
    if len(validURLPatternsList) < 1:
        return(articleList)
    else:
        # TODO: explore using list comprehension with filter:
        # valid_articles = [i for i in articleList if i in validURLPatternsList]
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
    """ Remove InValid Articles
    """
    valid_articles = []
    # TODO: explore using list comprehension with filter:
    # valid_articles = [i for i in articleList if i not in invalidURLPatternsList]
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
    """ Decode Secret, use Base64 for now, ignore the keyValue
    """
    decodedText = ""
    try:
        base64_bytes = encodedText.encode('ascii')
        decoded_bytes = base64.b64decode(base64_bytes)
        decodedText = decoded_bytes.decode('utf-8')

    except Exception as e:
        logger.error("Error decoding secret: %s", e)

    return(decodedText)


def saveObjToJSON(jsonFileName, objToSave):
    """ Save the object to a JSON format file
    """
    # don't catch any exception here, let it bubble up
    jsonString = json.dumps(objToSave)
    with open(jsonFileName, 'wt', encoding='utf-8') as fp:
        fp.write(jsonString)
        fp.close()


def checkAndSanitizeConfigString(configObj, sectionName, configParamName, default=None):
    """ Check and sanitize config string value """
    configParamValue = default
    try:
        paramStr = configObj.get(sectionName, configParamName).strip()
        configParamValue = paramStr
    except Exception as e:
        print("Error reading parameter '", configParamName, "' from configuration file, exception was:", e)
        if default is None:
            print("Error reading parameter '", configParamName, "' from configuration file: default value missing.")
    return(configParamValue)


def checkAndSanitizeConfigInt(configObj, sectionName, configParamName, default=None, maxValue=None, minValue=None):
    """ Check and sanitize config integer value """
    configParamValue = default
    try:
        paramVal = configObj.getint(sectionName, configParamName)
        if maxValue is not None:
            paramVal = min(paramVal, maxValue)
        if minValue is not None:
            paramVal = max(paramVal, minValue)
        configParamValue = paramVal
    except Exception as e:
        print("Error reading numeric parameter '",
              configParamName,
              "' from configuration file, exception was:",
              configParamName,
              e)
        if default is None:
            print("Error reading parameter '",
                  configParamName,
                  "' from configuration file: default value missing.")
    return(configParamValue)


def deDupeList(listWithDuplicates):
    """ Dedupe a given List by converting into a dict
    , and then re-converting to a list back again.
    """
    dedupedList = listWithDuplicates
    if type(listWithDuplicates).__name__ == 'list':
        dedupedList = list(
             OrderedDict.fromkeys(
                 listWithDuplicates
                )
            )
    return(dedupedList)


def spaceGapAfterDot(matchobj):
    """ Function called by fixSentenceGaps() when searching for sentence split checking regex to clean text """
    if matchobj is not None:
        if matchobj.group(0) == '-':
            return(' ')
        else:
            return(matchobj.group(1) + matchobj.group(2) + " " + matchobj.group(3))
    else:
        logger.error('Error extracting match data: empty object passed to function spaceGapAfterDot()')


def fixSentenceGaps(inputText):
    """ Searches for sentence split position and puts a space after the fullstop of a previous sentence. """
    return(re.sub(r'( [a-zA-Z]{2,})(\.)([A-Za-z]{2,} )', spaceGapAfterDot, inputText))


def filterRepeatedchars(baseText, charList):
    """ """
    cleanText = baseText
    for singleChar in charList:
        doubleChars = singleChar + singleChar
        while cleanText.find(doubleChars) > -1:
            cleanText = cleanText.replace(doubleChars, singleChar)
    return(cleanText)


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
    """ Cut source string between given substring Tags
    """
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
    """ Check and Parse Date String, set it to todays date if its in future
    """
    runDate = datetime.now()
    logger.debug("Checking date string: %s", dateStr)
    try:
        if type(dateStr).__name__ == 'datetime':
            runDate = dateStr
        elif type(dateStr).__name__ == 'str':
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


def getNextDaysDate(runDate):
    """ Given a date input, get date object of next day
    """
    businessDate = runDate
    try:
        if type(runDate).__name__ == 'datetime':
            businessDate = runDate + timedelta(days=1)
        else:
            logger.error("runDate parameter is of type: %s", type(runDate).__name__)
    except Exception as e:
        logger.error("While calculating date value of next day: %s", e)
    return(businessDate)


def getFullFilePathsInDir(directoryName):
    """ Get Files From Directory
    """
    filesList = []
    if os.path.isdir(directoryName) is True:
        filesList = [os.path.join(directoryName, i) for i in os.listdir(directoryName)
                     if os.path.isfile(os.path.join(directoryName, i))]
    return(filesList)


def getPreviousDaysDate(runDate):
    """ Given a date input, get date object of previous day
    """
    businessDate = runDate
    try:
        if type(runDate).__name__ == 'datetime':
            businessDate = runDate - timedelta(days=1)
        else:
            logger.error("runDate parameter is of type: %s", type(runDate).__name__)
    except Exception as e:
        logger.error("While calculating date value of previous day: %s", e)
    return(businessDate)


def readPluginNames(configObj):
    """ Read the list of plugins enabled in the configuration file
    """
    pluginList = []
    try:
        if 'plugins' in configObj.sections():
            section = configObj['plugins']
            if section.name == 'plugins':
                for key, item in section.items():
                    if key.startswith('plugin'):
                        pluginList.append(item)
    except Exception as e:
        logger.error("Error reading names of enabled plugins: %s", e)
    return(pluginList)


def loadPlugins(configData):
    """load only enabled plugins from the class files in the package directory
    """
    pluginsDict = dict()
    plugins_dir = configData['plugins_dir']
    contrib_plugins_dir = configData['plugins_contributed_dir']
    # add paths to load python files
    sys.path.append(configData['install_prefix'])
    sys.path.append(plugins_dir)
    sys.path.append(contrib_plugins_dir)
    # get list of plugins mentioned in the config file as comma separated list
    enabledPluginNames = readPluginNames(configData['configReader'])
    pluginList = []
    for listItem in enabledPluginNames:
        pluginList.append(removeStartTrailQuotes(listItem.strip()))
    # de-dupe list:
    enabledPluginNames = deDupeList(pluginList)
    modulesPackageName = os.path.basename(plugins_dir)
    for pluginFileName in importlib.resources.contents(modulesPackageName):
        # get full path:
        pluginFullPath = os.path.join(plugins_dir, pluginFileName)
        if os.path.isdir(pluginFullPath):  # skip directories
            continue
        # get only file name without file extension:
        modName = os.path.splitext(pluginFileName)[0]
        # only then load the module if module is enabled in the config file:
        if modName in enabledPluginNames:
            className = modName  # since class names of plugins are same as their module names
            try:
                logger.debug("Importing web-scraping plugin class: %s", modName)
                classObj = getattr(importlib.import_module(modName, package=modulesPackageName), className)
                pluginsDict[modName] = classObj()
            except Exception as e:
                logger.error("While importing plugin %s got exception: %s", modName, e)

    contribPluginsDict = loadPluginsContrib(configData, enabledPluginNames)
    pluginsDict.update(contribPluginsDict)
    return(pluginsDict)


def loadPluginsContrib(configData, enabledPluginNames):
    """
    load the contributed plugins for web-scraping
    """
    pluginsDict = dict()
    contrib_plugins_dir = configData['plugins_contributed_dir']
    sys.path.append(configData['install_prefix'])
    sys.path.append(contrib_plugins_dir)
    contribPluginsPackageName = os.path.basename(contrib_plugins_dir)
    for pluginFileName in importlib.resources.contents(contribPluginsPackageName):
        # get full path:
        pluginFullPath = os.path.join(contrib_plugins_dir, pluginFileName)
        if os.path.isdir(pluginFullPath):
            continue  # skip directories
        # extract only the file name without its file extension:
        modName = os.path.splitext(pluginFileName)[0]
        # only then load the module if module is enabled in the config file:
        if modName in enabledPluginNames:
            className = modName  # since class names of plugins are same as their module names
            try:
                logger.debug("Importing contributed web-scraping plugin class: %s", modName)
                classObj = getattr(importlib.import_module(modName, package=contribPluginsPackageName), className)
                pluginsDict[modName] = classObj()
            except Exception as e:
                logger.error("While importing contributed plugin %s got exception: %s", modName, e)
    return(pluginsDict)


def instClassFromFile(modulesPackageName, modName):
    """ Instantiate the class object from given file name
    """
    classInst = None
    className = modName  # since class names of plugins are same as their module names
    try:
        logger.debug("Instantiating web-scraping plugin class: %s", modName)
        classObj = getattr(importlib.import_module(modName, package=modulesPackageName), className)
        classInst = classObj()
    except Exception as e:
        logger.error("While instantiating plugin %s got exception: %s", modName, e)
    return(classInst)


def getNetworkLocFromURL(URLStr):
    """
    Derive network location from the given URL
    res.parsed_url contains the structure of the parsed URL
    """
    res = get_tld(URLStr, as_object=True)
    # Resulting object is: SplitResult(scheme = 'https', netloc = 'auto.economictimes.indiatimes.com'
    # , path = '/news/auto-components/abcd', query = '', fragment = '')
    return(res.parsed_url.netloc)


def checkIfURLIsValid(urlString):
    """ Check if URL is Valid """
    try:
        get_tld(urlString, as_object=True)
        return(True)
    except Exception as e:
        logger.debug("Invalid URL %s: %s", urlString, e)
        return(False)


def sameURLWithoutQueryParams(url1, url2):
    """
    Compare two URLs and return True if they are the same
    Ignore any Query parameters
    Example: SplitResult(scheme = 'https', netloc = 'auto.economictimes.indiatimes.com'
            , path = '/news/auto-components/abcd', query = '', fragment = '')
    """
    comparisonDecision = True
    try:
        result1 = get_tld(url1, as_object=True)
        result2 = get_tld(url2, as_object=True)
        if (result1.parsed_url.netloc == result2.parsed_url.netloc) and (
                result1.parsed_url.path == result2.parsed_url.path):
            comparisonDecision = True
        else:
            comparisonDecision = False
    except Exception as e:
        logger.debug("While comparing whether two URLs are the same, got exception: %s", e)
        logger.debug("url1 = %s, url2 = %s", url1, url2)
    return(comparisonDecision)


def extractLinks(url, docRoot):
    """ Extract all Links from beautifulSoup document object of HTML content
    Detect and fix URL errors, such as relative links starting with /
    Ignore invalid tags such as - javascript: , whatsappp:, mailto:, etc.
    """
    allLinks = []
    try:
        section = docRoot.find_all("a")
        rootTLDObj = get_tld(url, as_object=True)
        if section is not None and len(section) > 0:
            for tag in section:
                if tag.name == "a" and "href" in tag.attrs.keys():
                    linkValue = tag['href']
                    if linkValue.startswith('/'):
                        # convert to proper URL using network-location of the contianing page's url
                        allLinks.append(
                                        rootTLDObj.parsed_url.scheme
                                        + '://'
                                        + rootTLDObj.parsed_url.netloc
                                        + linkValue)
                    elif (linkValue.startswith("javascript:") is False and linkValue.startswith('mailto:') is False and
                          linkValue.startswith('#') is False and linkValue.startswith('?') is False and
                          linkValue.startswith('../') is False and
                          linkValue.startswith('http://http://') is False and linkValue.startswith('whatsapp:') is False):
                        allLinks.append(linkValue)
    except Exception as e:
        logger.error("Error extracting all Links from html document: %s", e)
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
    """
    Check if NLTK taggers and tokernzers are available.
    If not, then download the NLTK Data
    """
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
    """ use zlib's CRC32 function
    """
    crc = zlib.crc32(text) % (2 ** 32)
    return(hex(crc))


def printAppVersion():
    listOfGlobals = globals()
    print("Application version = ",
          listOfGlobals['app_inst'].appQueue.configData['version'])

# # end of file ##
