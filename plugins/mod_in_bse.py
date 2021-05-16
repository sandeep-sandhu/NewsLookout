#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_in_bse.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Plugin for the Bombay Stock Exchange Data, India



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
from datetime import datetime
import os
import logging

# import web retrieval and text processing python libraries:
# from bs4 import BeautifulSoup
# import lxml
import zipfile


# import this project's python libraries:
from base_plugin import basePlugin
from scraper_utils import getPreviousDaysDate
from data_structs import Types

##########

logger = logging.getLogger(__name__)


class mod_in_bse(basePlugin):
    """ Web Scraping plugin: mod_in_bse
    For Bombay Stock Exchange
    Country: India
    Save the BhavCopy file from the exchange at End of day
    """

    minArticleLengthInChars = 10000
    pluginType = Types.MODULE_DATA_CONTENT  # implies data content fetcher

    mainURL = 'https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_'
    mainURL_suffix = ".zip"

    pledgesURL1 = 'https://www.bseindia.com/data/xml/notices.xml'
    pledgesURL2 = 'https://www.bseindia.com/corporates/sastpledge.aspx'
    masterData = {}

    listOfURLS = []
    urlUniqueRegexps = ["(^https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_)([0-9]+)(.zip$)"]
    urlMatchPatterns = []
    uRLdata = dict()
    authorRegexps = []
    articleDateRegexps = dict()
    allowedDomains = ["www.bseindia.com"]
    validURLStringsToCheck = []
    invalidURLSubStrings = []
    nonContentURLs = []
    nonContentStrings = []

    def __init__(self):
        """ Initialize the object
        """
        self.count_history_to_fetch = 10
        super().__init__()

    def getURLsListForDate(self, runDate):
        """ Retrieve the URLs List For a given Date
        """
        self.listOfURLS = []
        try:
            businessDate = getPreviousDaysDate(runDate)
            urlForDate = self.mainURL + businessDate.strftime("%d%m%y") + self.mainURL_suffix
            self.listOfURLS = [urlForDate]
            logger.info("Total count of valid articles to be retrieved = %s for business date: %s",
                        len(self.listOfURLS), businessDate.strftime("%Y-%m-%d"))
        except Exception as e:
            logger.error("Error trying to retrieve listing of URLs: %s", e)

    def extractLinksFromURLList(self, runDate, listOfURLS):
        """ Generate history URLs by filling the default date pattern of the data file.
        """
        additionalLinks = []
        startDate = getPreviousDaysDate(runDate)
        # begin with previous date
        businessDate = startDate
        for dayCount in range(self.count_history_to_fetch):
            # decrement dates one by one
            businessDate = getPreviousDaysDate(businessDate)
            urlForDate = self.mainURL + businessDate.strftime("%d%m%y") + self.mainURL_suffix
            additionalLinks.append(urlForDate)
        logger.info("Generated links for %s dates", len(additionalLinks))
        return(additionalLinks)

    def fetchDataFromURL(self, uRLtoFetch, WorkerID):
        """ Fetch data From given URL
        """
        fullPathName = ""
        dirPathName = ""
        sizeOfDataDownloaded = -1
        uncompressSize = 0
        publishDateStr = ""

        logger.debug("Worker ID %s Fetching data from URL: %s", WorkerID, uRLtoFetch.encode("ascii"))
        # output tuple structure: (uRL, len_raw_data, len_text, publish_date)
        resultVal = (uRLtoFetch, None, None, None)
        try:
            (publishDate, dataUniqueID) = self.extractUniqueIDFromURL(uRLtoFetch)
            rawData = self.networkHelper.fetchRawDataFromURL(uRLtoFetch, type(self).__name__)
            # write data to file:
            fileNameWithOutExt = self.makeUniqueFileName(dataUniqueID)
            sizeOfDataDownloaded = len(rawData)
            if sizeOfDataDownloaded > self.minArticleLengthInChars:
                try:
                    publishDateStr = str(publishDate.strftime("%Y-%m-%d"))
                    dirPathName = os.path.join(self.configData['data_dir'], publishDateStr)
                    fullPathName = os.path.join(dirPathName, fileNameWithOutExt + ".zip")
                    # first check if directory of given date exists or not
                    if os.path.isdir(dirPathName) is False:
                        # since dir does not exist, so try creating it:
                        os.mkdir(dirPathName)
                except Exception as theError:
                    logger.error("When creating directory '%s', Exception was: %s", dirPathName, theError)
                try:
                    with open(fullPathName, 'wb') as fp:
                        n = fp.write(rawData)
                        logger.debug("Wrote %s bytes to file: %s", n, fullPathName)
                        fp.close()
                    uncompressSize = self.parseFetchedData(fullPathName,
                                                           dirPathName,
                                                           WorkerID,
                                                           str(publishDate.strftime("%Y%m%d")))
                except Exception as theError:
                    logger.error("When writing downloaded data to zip file '%s': %s", fullPathName, theError)
            else:
                logger.info("Ignoring data zip file '%s' since its size %s is less than %s bytes",
                            fullPathName, len(rawData), self.minArticleLengthInChars)
            # save count of characters of downloaded data for the given URL
            resultVal = (uRLtoFetch, sizeOfDataDownloaded, uncompressSize, publishDateStr)
        except Exception as e:
            logger.error("While fetching data, Exception was: %s", e)
        return(resultVal)

    def parseFetchedData(self, zipFileName, dataDirForDate, WorkerID, publishDateStr):
        """Parse the fetched Data
        """
        zipDatafile = None
        expandedSize = 0
        logger.debug("Expanding and parsing the fetched Zip archive, WorkerID = %s", WorkerID)
        zipDatafile = zipfile.ZipFile(zipFileName, mode='r')
        for memberZipInfo in zipDatafile.infolist():
            memberFileName = memberZipInfo.filename
            if memberZipInfo.filename.find('Readme.txt') < 0:
                try:
                    if os.path.isfile(os.path.join(dataDirForDate, memberFileName)) is False:
                        logger.debug("Extracting file '%s' from zip archive.", memberFileName)
                        zipDatafile.extract(memberZipInfo, path=dataDirForDate)
                    # rename extracted files to prefix the module's name:
                    if memberFileName.startswith('EQ_ISINCODE_'):
                        newName = os.path.join(dataDirForDate, "equity_bse_" + publishDateStr + '.csv')
                    else:
                        newName = os.path.join(dataDirForDate, type(self).__name__ + "_" + memberFileName)
                    os.rename(os.path.join(dataDirForDate, memberFileName),
                              newName)
                    expandedSize = expandedSize + memberZipInfo.file_size
                except Exception as e:
                    logger.error("Error extracting the fetched zip archive: %s", e)
        zipDatafile.close()
        # delete zip file as its no longer required:
        os.remove(zipFileName)
        return(expandedSize)

    def extractUniqueIDFromURL(self, URLToFetch):
        """ Get Unique ID From URL by extracting RegEx patterns matching any of urlMatchPatterns
        """
        # use today's date as default
        uniqueString = datetime.now().strftime('%d%m%y')
        date_obj = None

        if len(URLToFetch) > 6:
            for urlPattern in self.urlMatchPatterns:
                try:
                    result = urlPattern.search(URLToFetch)
                    uniqueString = result.group(2)
                    date_obj = datetime.strptime(uniqueString, '%d%m%y')
                    # if we did not encounter any error till this point, then this is the answer, so exit loop
                    break

                except Exception as e:
                    logger.debug("Error identifying unique ID of URL: %s , URL was: %s, Pattern: %s",
                                 e,
                                 URLToFetch.encode('ascii'),
                                 urlPattern)

        return((date_obj, uniqueString))

# # end of file ##
