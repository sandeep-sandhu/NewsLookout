#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_in_nse.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Copyright 2020, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com
 Country: India
 Purpose: Plugin for the National Stock Exchange Data, India
 Save the daily BhavCopy file generated by the exchange at the end of the day


 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.
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


class mod_in_nse(basePlugin):
    """ Web Scraping plugin: mod_in_nse
    For: National Stock Exchange
    """

    minArticleLengthInChars = 10000
    pluginType = Types.MODULE_DATA_CONTENT  # implies data content fetcher

    mainURL = 'https://www1.nseindia.com/archives/equities/bhavcopy/pr/PR'
    mainURL_suffix = ".zip"
    # URL pattern is: https://www1.nseindia.com/archives/equities/bhavcopy/pr/PRDDMMYY.zip

    listOfURLS = []
    urlUniqueRegexps = ["(^https://www1.nseindia.com/archives/equities/bhavcopy/pr/PR)([0-9]+)(.zip$)"]
    urlMatchPatterns = []
    uRLdata = dict()
    authorRegexps = []
    articleDateRegexps = {}
    validURLStringsToCheck = []
    invalidURLSubStrings = []
    nonContentURLs = []
    allowedDomains = ["www1.nseindia.com", "www.nseindia.com"]

    def __init__(self):
        """ Initialize the object
        """
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

    def extractLinks3LevelsDeep(self, runDate):
        """ Generate history URLs by filling the default date pattern of the data file.
        """
        startDate = getPreviousDaysDate(runDate)
        # begin with previous date
        businessDate = startDate

        for dayCount in range(365 * 10):
            # decrement dates one by one
            businessDate = getPreviousDaysDate(businessDate)

            urlForDate = self.mainURL + businessDate.strftime("%d%m%y") + self.mainURL_suffix

            self.listOfURLS.append(urlForDate)

        logger.info("Generated links for %s dates", len(self.listOfURLS))
        self.tempSaveLinksIdentified(self.listOfURLS)

    def fetchDataFromURL(self, uRLtoFetch, WorkerID):
        """ Fetch data From given URL
        """
        fullPathName = ""
        dirPathName = ""
        sizeOfDataDownloaded = -1
        uncompressSize = 0
        publishDateStr = ""

        # output tuple structure: (uRL, len_raw_data, len_text, publish_date)
        resultVal = (uRLtoFetch, None, None, None)

        logger.debug("Fetching %s, Worker ID %s", uRLtoFetch.encode("ascii"), WorkerID)

        try:
            (publishDate, dataUniqueID) = self.extractUniqueIDFromURL(uRLtoFetch)

            rawData = self.networkHelper.fetchRawDataFromURL(uRLtoFetch, type(self).__name__)

            # write data to file:
            fileNameWithOutExt = self.makeUniqueFileName(dataUniqueID)

            publishDateStr = str(publishDate.strftime("%Y-%m-%d"))

            dirPathName = os.path.join(self.configData['data_dir'], publishDateStr)

            fullPathName = os.path.join(dirPathName, fileNameWithOutExt + ".zip")

            sizeOfDataDownloaded = len(rawData)

        except Exception as e:
            logger.error("Trying to fetch data from given URL: %s", e)

        if sizeOfDataDownloaded > self.minArticleLengthInChars:
            try:
                # first check if directory of given date exists or not
                if os.path.isdir(dirPathName) is False:
                    # since dir does not exist, so try creating it:
                    os.mkdir(dirPathName)

            except Exception as theError:
                logger.error("Error creating directory '%s', Exception was: %s", dirPathName, theError)

            try:
                with open(fullPathName, 'wb') as fp:
                    n = fp.write(rawData)
                    logger.debug("Wrote %s bytes to file: %s", n, fullPathName)
                    fp.close()

                uncompressSize = self.parseFetchedData(fullPathName, dirPathName, WorkerID)

            except Exception as theError:
                logger.error("Error writing downloaded data to zip file '%s': %s", fullPathName, theError)

            # save count of characters of downloaded data for the given URL:
            # (uRL, len_raw_data, len_text, publish_date)
            resultVal = (uRLtoFetch, sizeOfDataDownloaded, uncompressSize, publishDateStr)

        else:
            logger.info("Ignoring data zip file '%s' since its size %s is less than %s bytes",
                        fullPathName, len(rawData), self.minArticleLengthInChars)
        return(resultVal)

    def parseFetchedData(self, zipFileName, dataDirForDate, WorkerID):
        """Parse the fetched Data
        """
        zipDatafile = None
        uncompressSize = 0
        logger.debug("Expanding and parsing the fetched Zip archive, WorkerID = %s", WorkerID)

        zipDatafile = zipfile.ZipFile(zipFileName, mode='r')

        for memberZipInfo in zipDatafile.infolist():
            if memberZipInfo.filename.find('Readme.txt') < 0:
                try:
                    logger.debug("Extracting file '%s' from zip archive.", memberZipInfo.filename)

                    zipDatafile.extract(memberZipInfo, path=dataDirForDate)
                    # rename extracted files to prefix the module's name:
                    logger.debug("Renaming %s to %s",
                                 os.path.join(dataDirForDate, memberZipInfo.filename),
                                 os.path.join(dataDirForDate, type(self).__name__ + "_" + memberZipInfo.filename))

                    os.rename(os.path.join(dataDirForDate, memberZipInfo.filename),
                              os.path.join(dataDirForDate, type(self).__name__ + "_" + memberZipInfo.filename))

                    uncompressSize = uncompressSize + memberZipInfo.file_size

                except Exception as e:
                    logger.error("Error parsing the fetched zip archive: %s", e)

        zipDatafile.close()

        # delete zip file as its no longer required:
        os.remove(zipFileName)

        return(uncompressSize)

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
