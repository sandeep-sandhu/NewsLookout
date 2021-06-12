#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_dedupe.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-01
 Purpose: Plugin for de-duplication of articles
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
import os
import time
from tqdm import tqdm
# import numpy as np

# import this project's python libraries:
from base_plugin import basePlugin
from data_structs import Types, NewsArticle
from scraper_utils import getFullFilePathsInDir
# from scraper_utils import getNextDaysDate, getPreviousDaysDate

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import spacy

##########

logger = logging.getLogger(__name__)

###########


class mod_dedupe(basePlugin):
    """ Web Scraping plugin: mod_dedupe
    For de-duplicating already downloaded data
    """
    minArticleLengthInChars = 400
    pluginType = Types.MODULE_DATA_PROCESSOR  # implies data post-processor

    listOfFiles = []
    uRLdata = dict()

    def __init__(self):
        """ Initialize the object """
        super().__init__()

    def setConfig(self, configDict):
        self.workDir = configDict['data_dir']

    def setupModel(self):
        """
        Setup the NLP models for computing similarity.
        """
        try:
            self.nlpModel = spacy.load("en_core_web_lg")
        except Exception as e:
            logger.error("Error loading the NLP model for de-dupe: %s", e)

    def processData(self, runDate):
        """ Process data for runDate
        :param runDate: The business date for which the text needs to be processed.
        :type runDate: datetime
        """
        logging.captureWarnings(True)
        self.setupModel()
        print("\nData de-duplication progress:")
        # find list of articles newly fetched:
        listOfFiles = self.identifyFilesForDate(runDate)
        logger.info("Identified %s files to be de-duplicated for date: %s",
                    len(listOfFiles), runDate.strftime('%Y-%m-%d'))
        # load each article one by one, and compare with other articles:
        deletedCount = 0
        startTime = time.time()
        for fileIndex1, file1 in enumerate(listOfFiles):
            try:
                stopTime = time.time()
                statusBarText = tqdm.format_meter(fileIndex1 + 1,
                                                  len(listOfFiles),
                                                  stopTime - startTime,
                                                  ncols=80,
                                                  ascii=False)
                print(statusBarText, '\b' * 100, end='')
                startTime = time.time()
                # load document data from fileName and compute its text embedding:
                document1 = self.computeTextEmbeddingDoc(file1)
                if document1 is not None:
                    for fileIndex2 in range(fileIndex1 + 1, len(listOfFiles)):
                        document2 = self.computeTextEmbeddingDoc(listOfFiles[fileIndex2])
                        logger.debug('de-dupe %s -> %s', file1, listOfFiles[fileIndex2])
                        resultTuple = self.compareTwoArticles(document1, document2,
                                                              compareThreshold=0.99, maxSizePercentDiff=0.20)
                        # check that first article's file exist, only then delete the second article:
                        if resultTuple is not None and os.path.isfile(resultTuple[1].getFileName()):
                            self.removeArticle(resultTuple[2])
                            deletedCount = deletedCount + 1
            except Exception as e:
                logger.error("Error identifying similar files: %s, file compared: %s", e, file1)
        print('')
        logger.info('\nDeleted %s duplicates.', deletedCount)

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

    def identifyFilesForDate(self, runDate):
        """ Get list of files for directories for tomorow's run-date, today's run date and yesterday's run-date
        :param runDate: The business date for which the text needs to be processed.
        :type runDate: datetime
        :rtype List[str]
        """
        runDateString = runDate.strftime("%Y-%m-%d")
        listOfFiles = getFullFilePathsInDir(self.identifyDataPathForRunDate(runDateString))
        # # get articles from previous day too:
        # listOfFiles = listOfFiles + getFullFilePathsInDir(
        #     self.identifyDataPathForRunDate(getPreviousDaysDate(runDateString)))
        # # get for next day, in case data is available:
        # listOfFiles = listOfFiles + getFullFilePathsInDir(
        #     self.identifyDataPathForRunDate(getNextDaysDate(runDateString)))
        # remove non-json files:
        newlist = [i for i in listOfFiles if i.endswith('json')]
        return(newlist)

    def compareTwoArticles(self, doc1, doc2, compareThreshold=0.99, maxSizePercentDiff=0.20):
        """ Compare two articles using their text embeddings.
        If the similarity score is <compareThreshold> or more, then check size difference.
        If size difference is less than maxSizePercentDiff
        """
        similarityScore = 0.0
        resultTuple = None
        try:
            if doc1 is not None and doc2 is not None:
                smallerLen = min(doc1.getTextSize(), doc2.getTextSize())
                biggerLen = max(doc1.getTextSize(), doc2.getTextSize())
                percentDiff = (biggerLen-smallerLen)*1.0/biggerLen
                # Proceed with compute intensive similarity checking only if
                # the percentage size difference is not more than maxSizePercentDiff
                # and both documents are from different publications:
                if (percentDiff <= maxSizePercentDiff and
                        doc1.getModuleName() != doc2.getModuleName()):
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

    def computeTextEmbeddingDoc(self, fileName):
        """ Calculate Text Embedding
        """
        document = None
        minAcceptableTextLength = 30
        try:
            if os.path.isfile(fileName):
                document = NewsArticle()
                # load data from fileName:
                document.readFromJSON(fileName)
                document.setFileName(fileName)
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
            # calculate .html.bz2 filename, check if exists, delete it:
            htmlFileName = articleObject.getFileName().replace('.json', '.html.bz2')
            if os.path.isfile(htmlFileName):
                logger.debug("Deleting duplicate article's HTML file: %s", htmlFileName)
                os.remove(htmlFileName)
        except Exception as e:
            logger.error("Error: %s", e)

    def examineSampleDocs(self, similarTuples, printLimit):
        """ Print sample list of similar documents on screen
        """
        counter = 0
        for index, record in enumerate(similarTuples):
            score = record[0]
            if record[1].urlData["module"] != record[2].urlData["module"]:
                text1 = record[1].getText()
                text2 = record[2].getText()
                print('\n---Example', index, 'with similarity score =', score)
                print(" _____Text 1:", record[1].urlData["module"], ', ID =', record[1].getArticleID(), "\n", text1)
                print(" _____Text 2:", record[2].urlData["module"], ', ID =', record[2].getArticleID(), "\n", text2)
                counter = counter + 1
            if counter > printLimit:
                break

    def makeDocPairs(self, listOfFiles):
        """ For each file in listOfFiles, make a unique pair with all other files.
        Return this list of pair-wise tuples.
        """
        allDocuments = []
        allPairsList = []
        for fileName in listOfFiles:
            try:
                # load document data from fileName and compute its text embedding:
                document = self.computeTextEmbeddingDoc(fileName)
                if document is not None:
                    allDocuments.append(document)
            except Exception as e:
                logger.error("Error calculating text embedding: %s, file: %s", e, fileName)
        # make pairs of all files, remove duplicates since order does not matter
        try:
            allPairsList = [(allDocuments[i], allDocuments[j]) for i in range(len(allDocuments))
                            for j in range(i+1, len(allDocuments))]
        except Exception as e:
            logger.error("Error making pairs of documents: %s", e)
        return(allPairsList)

    def compareAllDocsInList(self, allPairsList, compareThreshold=0.99):
        """ For each file in allPairsList, load the document text.
        Calculate text representation for this document.
        In a loop, compare the text representation of each file with all others
        Save and return the scores in a list of Tuples.
        """
        listOfTuples = []
        try:
            logger.debug("Started calculating pair-wise similarity scores for all documents")
            for docPair in allPairsList:
                resultTuple = self.compareTwoArticles(docPair[0], docPair[1], compareThreshold=compareThreshold)
                listOfTuples.append(resultTuple)
        except Exception as e:
            logger.error("Error comparing documents pair-wise: %s", e)
        return(listOfTuples)


# # end of file ##
