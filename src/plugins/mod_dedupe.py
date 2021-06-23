#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
#                                                                                                         #
# File name: mod_dedupe.py                                                                                #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Plugin for de-duplication of articles                                                          #
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
import time
from datetime import datetime

# import this project's python libraries:
from base_plugin import BasePlugin
from data_structs import Types

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import spacy

##########

logger = logging.getLogger(__name__)

###########


class mod_dedupe(BasePlugin):
    """ Web Scraping plugin: mod_dedupe
    For de-duplicating already downloaded data
    """
    minArticleLengthInChars = 400
    pluginType = Types.MODULE_DATA_PROCESSOR  # implies data post-processor

    listOfFiles = []
    uRLdata = dict()

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
        self.setupModel()

    def setupModel(self):
        """
        Setup the NLP models for computing similarity.
        """
        try:
            # prefer to use GPU if available:
            spacy.prefer_gpu()
            spacymodel = self.app_config.checkAndSanitizeConfigString('plugins', 'mod_dedupe_spacymodel')
            self.nlpModel = spacy.load(spacymodel)
        except Exception as e:
            logger.error("Error loading the NLP model for de-dupe: %s", e)

    def processDataObj(self, newsEventObj):
        """ Process data in the given data object with this plugin.

        :param newsEventObj: The ExecutionResult object to be processed.
        :type newsEventObj: data_structs.ExecutionResult
        :param runDate: The business date for which the text needs to be processed.
        :type runDate: datetime.datetime
        """
        runDate = datetime.strptime(newsEventObj.getPublishDate(), '%Y-%m-%d')
        logger.info("Started data de-duplication for data in file: %s, for date: %s",
                    newsEventObj.getFileName(), runDate)
        # find list of articles newly fetched:
        listOfFiles = self.identifyFilesForDate(self.app_config.data_dir, runDate)
        logger.info("Identified %s files to be checked for similarity for date: %s",
                    len(listOfFiles)-1, runDate.strftime('%Y-%m-%d'))
        # load each article one by one, and compare with other articles:
        deletedCount = 0
        currCounter = 1
        startTime = time.time()
        totalComparisonCount = len(listOfFiles)-1
        totalTime = 0.0
        # if this is valid data, then compute its text embedding:
        if newsEventObj is not None:
            document1 = self.computeTextEmbeddingDoc(newsEventObj)
            # compare with all other files for given date
            for fileIndex2, file2 in enumerate(listOfFiles):
                try:
                    if file2 != newsEventObj.getFileName():
                        currCounter = currCounter + 1
                        stopTime = time.time()
                        timeDiff = stopTime - startTime
                        totalTime = totalTime + timeDiff
                        startTime = time.time()
                        logger.debug('Checking similarity of: %s -> %s', newsEventObj.getFileName(), file2)
                        resultTuple = self.compareTwoArticles(document1,
                                                              file2,
                                                              compareThreshold=0.99,
                                                              maxSizePercentDiff=0.20)
                        # check that first article's file exist, only then delete the second article:
                        if resultTuple is not None and os.path.isfile(resultTuple[1].getFileName()):
                            self.removeArticle(resultTuple[2])
                            deletedCount = deletedCount + 1
                except Exception as e:
                    logger.error(f"Error comparing files: {newsEventObj.getFileName()} vs. {file2}")
        if deletedCount > 0:
            logger.info(f'Deleted {deletedCount} duplicate news events.')

    def processAllDataFiles(self, runDate):
        """ Process data for runDate.

        :param runDate: The business date for which the text needs to be processed.
        :type runDate: datetime.datetime
        """
        logging.captureWarnings(True)
        print("Data de-duplication progress:")
        # find list of articles newly fetched:
        listOfFiles = self.identifyFilesForDate(self.app_config.data_dir, runDate)
        logger.info("Identified %s files to be de-duplicated for date: %s",
                    len(listOfFiles), runDate.strftime('%Y-%m-%d'))
        # load each article one by one, and compare with other articles:
        deletedCount = 0
        currCounter = 1
        startTime = time.time()
        totalComparisonCount = len(listOfFiles) * (len(listOfFiles)-1.0)/2.0
        totalTime = 0.0
        for fileIndex1, file1 in enumerate(listOfFiles):
            try:
                # load document data from fileName and compute its text embedding:
                document1 = self.loadDocument(file1)
                if document1 is not None:
                    document1 = self.computeTextEmbeddingDoc(document1)
                    for fileIndex2 in range(fileIndex1 + 1, len(listOfFiles)):
                        currCounter = currCounter + 1
                        stopTime = time.time()
                        timeDiff = stopTime - startTime
                        totalTime = totalTime + timeDiff
                        # statusBarText = tqdm.format_meter(currCounter,
                        #                                   totalComparisonCount,
                        #                                   totalTime,
                        #                                   ncols=80,
                        #                                   ascii=False)
                        # print(statusBarText, '\b' * 100, end='')
                        startTime = time.time()
                        logger.debug('de-dupe %s -> %s', file1, listOfFiles[fileIndex2])
                        resultTuple = self.compareTwoArticles(document1, listOfFiles[fileIndex2],
                                                              compareThreshold=0.99, maxSizePercentDiff=0.20)
                        # check that first article's file exist, only then delete the second article:
                        if resultTuple is not None and os.path.isfile(resultTuple[1].getFileName()):
                            self.removeArticle(resultTuple[2])
                            deletedCount = deletedCount + 1
            except Exception as e:
                logger.error("Error identifying similar files: %s, file compared: %s", e, file1)
        # print(tqdm.format_meter(totalComparisonCount, totalComparisonCount, totalTime, ncols=80))
        logger.info('Deleted a total of %s duplicate news articles.', deletedCount)

    def deleteDuplicateFiles(self, similarTuples):
        """ Delete duplicate files for each set of duplicates identified
        """
        # for each pair, delete the second document, i.e.: recTuple[index][2]
        if similarTuples is not None:
            for counter, recTuple in enumerate(similarTuples):
                try:
                    # check that first record files exist, only then delete the second record:
                    if os.path.isfile(recTuple[1].getFileName()):
                        self.removeArticle(recTuple[2])
                except Exception as e:
                    logger.error("Error deleting duplicate files for %s tuple %s: %s", counter, recTuple, e)
        return(counter + 1)

    def compareTwoArticles(self, doc1, fileName2, compareThreshold=0.99, maxSizePercentDiff=0.20):
        """ Compare two articles using their text embeddings.
        If the similarity score >= compareThreshold, then check size difference.
        If size difference is less than maxSizePercentDiff, then mark as duplicates.
        """
        similarityScore = 0.0
        resultTuple = None
        try:
            doc2 = self.loadDocument(fileName2)
            if doc1 is not None and doc2 is not None:
                smallerLen = min(doc1.getTextSize(), doc2.getTextSize())
                biggerLen = max(doc1.getTextSize(), doc2.getTextSize())
                percentDiff = (biggerLen-smallerLen)*1.0/biggerLen
                # Proceed with compute intensive similarity checking only if
                # the percentage size difference is not more than maxSizePercentDiff
                # and both documents are from different publications:
                if (percentDiff <= maxSizePercentDiff and
                        doc1.getModuleName() != doc2.getModuleName()):
                    doc2 = self.computeTextEmbeddingDoc(doc2)
                    # Calculate the similarity score of doc1 vs. doc2:
                    similarityScore = doc1.getTextEmbedding().similarity(doc2.getTextEmbedding())
                    # return set of tuples whose value of similarity score exceeds the threshold (i.e. compareThreshold)
                    logger.debug("Similarity of doc 1 vs. 2 = %s, Percentage size diff = %s", similarityScore, percentDiff)
                    if similarityScore >= compareThreshold:
                        # find older document and place it second in the tuple to indicate it will be deleted,
                        # or else, for same date, delete the smaller document
                        if doc1.getTextSize() > doc2.getTextSize():
                            resultTuple = (similarityScore, doc1, doc2)
                        else:
                            resultTuple = (similarityScore, doc2, doc1)
            else:
                return(None)
        except Exception as e:
            logger.error("Error trying to calculate similarity of documents: %s", e)
        return(resultTuple)

    def computeTextEmbeddingDoc(self, document):
        """ Calculate Text Embedding
        """
        minAcceptableTextLength = 30
        try:
            if document.getTextSize() > minAcceptableTextLength:
                # logger.debug("Generating text representation of the document for file %s", fileName)
                textEmbedding = self.nlpModel(document.getText())
                document.setTextEmbedding(textEmbedding)
            else:
                document = None
        except Exception as e:
            logger.error("Error trying to calculate similarity of URLs: %s", e)
        return(document)

    def removeArticle(self, articleObject):
        """ Remove article identified as duplicate
        """
        try:
            logger.debug("Removing files for module: %s, Article ID = %s",
                         articleObject.getModuleName(),
                         articleObject.getArticleID())
            if os.path.isfile(articleObject.getFileName()):
                # delete it:
                logger.info("Deleting duplicate article's json file: %s, for URL: %s",
                            articleObject.getFileName(), articleObject.getURL())
                os.remove(articleObject.fileName)
                self.sessionHistDB.addDupURLToDeleteTbl(articleObject.getURL(),
                                                        articleObject.getModuleName(),
                                                        articleObject.getPublishDate(),
                                                        articleObject.getFileName())
            # calculate .html.bz2 filename, check if exists, delete it:
            htmlFileName = articleObject.getFileName().replace('.json', '.html.bz2')
            if os.path.isfile(htmlFileName):
                logger.debug("Deleting duplicate article's HTML file: %s", htmlFileName)
                os.remove(htmlFileName)
        except Exception as e:
            logger.error("Error: %s", e)


# # end of file ##
