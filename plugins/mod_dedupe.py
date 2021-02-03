#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: mod_dedupe.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Plugin for de-duplication of articles
"""

__version__ = "1.6"
__author__ = "Sandeep Singh Sandhu"
__copyright__ = "Copyright 2020, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu"
__credits__ = ["Sandeep Singh Sandhu"]
__license__ = "GPL"
__maintainer__ = "Sandeep Singh Sandhu"
__email__ = "sandeep.sandhu@gmx.com"
__status__ = "Production"

####################################

# import standard python libraries:
from datetime import date, datetime
import time
import random
import re
from ast import parse

import logging
logger = logging.getLogger(__name__)

# import web retrieval and text processing python libraries:
from bs4 import BeautifulSoup
import newspaper
from newspaper import Article, Source
import requests
from urllib3.exceptions import InsecureRequestWarning
import nltk
import lxml
import cchardet
import spacy
import nltk


# import this project's python libraries:
from baseModule import baseModule
from scraper_utils import normalizeURL, NewsArticle, cutStrBetweenTags, cutStrFromTag, calculateCRC32
from scraper_utils import retainValidArticles, removeInValidArticles
from scraper_utils import Types


####################################


class mod_dedupe(baseModule):
    """ Web Scraping plugin: mod_dedupe
    For de-duplicating already downloaded data
    
    """
    
    minArticleLengthInChars = 400
    moduleType = Types.MODULE_DATA_PROCESSOR # implies data post-processor


    listOfURLS = []
    urlUniqueRegexps = []
    urlMatchPatterns = []
    uRLdata = dict()
    
    
    def __init__(self):
        """ Initialize the object """
        
        super().__init__()




    def compareTwoArticles(self, text1, text2):
        """ compare """
        
        similarityResult = 0
        
        logger.debug( "Comparing article texts" )
        
        try:

            

            nlp = spacy.load("en_core_web_lg")
            doc1 = nlp(text1)
            doc2 = nlp(text2)
            
            
            # Similarity of two documents
            similarityResult = doc1.similarity(doc2)
            
            logger.debug("Similarity of doc1 with doc2 is: %s", similarityResult)

            
        except Exception as e:
            logger.error("Error trying to calculate similarity of URLs: %s", e )




    
    

## end of file ##