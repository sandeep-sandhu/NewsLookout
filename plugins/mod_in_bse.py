#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_in_bse.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
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
import requests

# import this project's python libraries:
from base_plugin import basePlugin
from scraper_utils import getPreviousDaysDate
from data_structs import Types, ExecutionResult

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
    master_data_dir = None

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

    def getURLsListForDate(self, runDate, completedURLs):
        """ Retrieve the URLs List For a given Date
        """
        self.listOfURLS = []
        try:
            # set dates for retrieval based on recursion level in configuration file:
            if self.configData['recursion_level'] == 2:
                self.count_history_to_fetch = 10
            elif self.configData['recursion_level'] == 3:
                self.count_history_to_fetch = 31
            elif self.configData['recursion_level'] > 3:
                self.count_history_to_fetch = 3650
            else:
                self.count_history_to_fetch = 1
            # begin with run date:
            businessDate = runDate
            for dayCount in range(self.count_history_to_fetch):
                # decrement dates one by one
                businessDate = getPreviousDaysDate(businessDate)
                urlForDate = self.mainURL + businessDate.strftime("%d%m%y") + self.mainURL_suffix
                self.listOfURLS.append(urlForDate)
            # remove already retrieved URLs:
            self.listOfURLS = completedURLs.removeAlreadyFetchedURLs(self.listOfURLS, self.pluginName)
            logger.info("Total count of valid articles to be retrieved = %s for business date: %s",
                        len(self.listOfURLS), businessDate.strftime("%Y-%m-%d"))
        except Exception as e:
            logger.error("Error trying to retrieve URL list at recursion level %s: %s",
                         self.configData['recursion_level'], e)
        self.putQueueEndMarker()

    def fetchDataFromURL(self, uRLtoFetch, WorkerID):
        """ Fetch data From given URL
        """
        self.pluginState = Types.STATE_FETCH_CONTENT
        fullPathName = ""
        dirPathName = ""
        sizeOfDataDownloaded = -1
        uncompressSize = 0
        publishDateStr = ""
        self.master_data_dir = self.configData['master_data_dir']
        logger.debug("Worker ID %s Fetching data from URL: %s", WorkerID, uRLtoFetch.encode("ascii"))
        resultVal = None
        try:
            (publishDate, dataUniqueID) = self.extractUniqueIDFromURL(uRLtoFetch)
            rawData = self.downloadDataArchive(uRLtoFetch, type(self).__name__)
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
                    uncompressSize = uncompressSize + self.fetchPledgesData(self.master_data_dir, publishDate)
                except Exception as theError:
                    logger.error("Error saving downloaded data to zip file '%s': %s", fullPathName, theError)
            else:
                logger.info("Ignoring data zip file '%s' since its size %s is less than %s bytes",
                            fullPathName, len(rawData), self.minArticleLengthInChars)
            # save metrics/count of downloaded data for the given URL
            resultVal = ExecutionResult(uRLtoFetch,
                                        sizeOfDataDownloaded,
                                        uncompressSize,
                                        publishDateStr,
                                        self.pluginName)
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
                    # rename extracted files to prefix the module's name:
                    if memberFileName.startswith('EQ_ISINCODE_'):
                        newName = os.path.join(dataDirForDate, "equity_bse_" + publishDateStr + '.csv')
                    else:
                        newName = os.path.join(dataDirForDate, type(self).__name__ + "_" + memberFileName)
                    if os.path.isfile(newName) is False:
                        logger.debug("Extracting file '%s' from zip archive.", memberFileName)
                        zipDatafile.extract(memberZipInfo, path=dataDirForDate)
                    os.rename(os.path.join(dataDirForDate, memberFileName), newName)
                    expandedSize = expandedSize + memberZipInfo.file_size
                except Exception as e:
                    logger.error("Error extracting the fetched zip archive: %s", e)
        zipDatafile.close()
        os.remove(zipFileName)  # delete zip file since its no longer required
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

    def fetchPledgesData(self, dirPathName, publishDate):
        """ fetch Pledges Data"""
        resp = None
        fetchTimeout = self.configData['fetch_timeout']
        try:
            with requests.Session() as sess:
                sess.proxies.update(self.configData['proxies'])
                resp = sess.get(self.pledgesURL2, timeout=fetchTimeout)
                # write: resp.content
        except Exception as e:
            logger.error('Error getting pledges data: %s', e)
        return(resp.content)


# # end of file ##
