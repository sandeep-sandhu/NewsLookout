#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_dedupe.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Plugin for de-duplication of articles
 Copyright 2020, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com


 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.

"""

##########

# import standard python libraries:
import logging

# import web retrieval and text processing python libraries:
# import nltk
# import lxml
# import cchardet
# import spacy

# import this project's python libraries:
from base_plugin import basePlugin
from data_structs import Types
# from data_structs import NewsArticle

##########

logger = logging.getLogger(__name__)


class mod_dedupe(basePlugin):
    """ Web Scraping plugin: mod_dedupe
    For de-duplicating already downloaded data
    """

    minArticleLengthInChars = 400

    pluginType = Types.MODULE_DATA_PROCESSOR  # implies data post-processor

    listOfURLS = []
    urlUniqueRegexps = []
    urlMatchPatterns = []
    uRLdata = dict()

    def __init__(self):
        """ Initialize the object """
        super().__init__()

    def processData(self, runDate):
        """ process data """
        # find list of artciles newly fetched
        # load each article one by one
        # for each article loaded, compare with other articles within +/- 10 days
        # highlight newer article as duplicate
        # for same date but different sources, highlight shorter article as duplicate
        pass

    def compareTwoArticles(self, text1, text2):
        """ Compare two articles
        """
        try:
            similarityResult = 0
            logger.debug("Comparing article texts")
            # nlp = spacy.load("en_core_web_lg")
            # doc1 = nlp(text1)
            # doc2 = nlp(text2)
            # # Similarity of two documents
            # similarityResult = doc1.similarity(doc2)
            logger.debug("Similarity of doc1 with doc2 is: %s", similarityResult)
        except Exception as e:
            logger.error("Error trying to calculate similarity of URLs: %s", e)

# # end of file ##
