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

# import web retrieval and text processing python libraries:
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem import PorterStemmer
from num2words import num2words
import numpy as np
import gensim


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
            logger.debug("Processed text: %s", self.preprocess(text1))

            logger.debug("Similarity of doc1 with doc2 is: %s", similarityResult)
        except Exception as e:
            logger.error("Error trying to calculate similarity of URLs: %s", e)

    def convert_lower_case(self, data):
        return np.char.lower(data)

    def remove_stop_words(self, data):
        stop_words = stopwords.words('english')
        words = word_tokenize(str(data))
        new_text = ""
        for w in words:
            if w not in stop_words and len(w) > 1:
                new_text = new_text + " " + w
        return new_text

    def remove_punctuation(self, data):
        symbols = r"!\"#$%&()*+-./:;<=>?@[\]^_`{|}~\n"
        for i in range(len(symbols)):
            data = np.char.replace(data, symbols[i], ' ')
            data = np.char.replace(data, "  ", " ")
        data = np.char.replace(data, ',', '')
        return data

    def remove_apostrophe(self, data):
        return np.char.replace(data, "'", "")

    def stemming(self, data):
        stemmer = PorterStemmer()
        tokens = word_tokenize(str(data))
        new_text = ""
        for w in tokens:
            new_text = new_text + " " + stemmer.stem(w)
        return new_text

    def convert_numbers(self, data):
        tokens = word_tokenize(str(data))
        new_text = ""
        for w in tokens:
            try:
                w = num2words(int(w))
            except Exception as e:
                logger.error("Error converting numbers: %s", e)
            new_text = new_text + " " + w
        new_text = np.char.replace(new_text, "-", " ")
        return new_text

    def preprocess(self, data):
        data = self.convert_lower_case(data)
        data = self.remove_punctuation(data)  # remove comma seperately
        data = self.remove_apostrophe(data)
        data = self.remove_stop_words(data)
        data = self.convert_numbers(data)
        data = self.stemming(data)
        data = self.remove_punctuation(data)
        data = self.convert_numbers(data)
        data = self.stemming(data)  # needed again as we need to stem the words
        data = self.remove_punctuation(data)  # needed again as num2word is giving few hypens and commas fourty-one
        data = self.remove_stop_words(data)  # needed again as num2word is giving stop words 101 - one hundred and one
        return data

    def similarityNLTK(self, text1, text2):
        #
        file_docs = []
        tokens = sent_tokenize(text1)
        for line in tokens:
            file_docs.append(line)
        gen_docs = [[w.lower() for w in word_tokenize(text)] for text in file_docs]
        dictionary = gensim.corpora.Dictionary(gen_docs)
        print(dictionary.token2id)
        corpus = [dictionary.doc2bow(gen_doc) for gen_doc in gen_docs]
        tf_idf = gensim.models.TfidfModel(corpus)
        for doc in tf_idf[corpus]:
            print([[dictionary[id], np.around(freq, decimals=2)] for id, freq in doc])
        # building the index
        sims = gensim.similarities.Similarity('workdir/', tf_idf[corpus],
                                              num_features=len(dictionary))
        file2_docs = []
        tokens = sent_tokenize(text2)
        for line in tokens:
            file2_docs.append(line)
        print("Number of documents:", len(file2_docs))
        for line in file2_docs:
            query_doc = [w.lower() for w in word_tokenize(line)]
            query_doc_bow = dictionary.doc2bow(query_doc)  # update an existing dictionary and create bag of words
        # perform a similarity query against the corpus
        query_doc_tf_idf = tf_idf[query_doc_bow]
        # print(document_number, document_similarity)
        print('Comparing Result:', sims[query_doc_tf_idf])
        sum_of_sims = (np.sum(sims[query_doc_tf_idf], dtype=np.float32))
        print(sum_of_sims)
        percentage_of_similarity = round(float((sum_of_sims / len(file_docs)) * 100))
        return(percentage_of_similarity)


# # end of file ##
