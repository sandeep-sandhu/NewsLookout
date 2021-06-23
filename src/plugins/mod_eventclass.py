#!/usr/bin/env python
# -*- coding: utf-8 -*-

# ###########################################################################################################
#                                                                                                           #
# File name: mod_eventclass.py                                                                              #
# Application: The NewsLookout Web Scraping Application                                                     #
# Date: 2021-06-10                                                                                          #
# Purpose: Plugin for classification of news events as positive, negative, or neutral                       #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com    #
#                                                                                                           #
# The finBERT model used for event classification is attributed to:                                         #
# Y Yang, K. Z. Zhang P.K. Kannan, Mark Christopher Siy Uy and Allen Huang.                                 #
# “FinBERT: A Pretrained Language Model for Financial Communications.” ArXiv abs/2006.08097 (2020): n. pag. #
# URL: https://arxiv.org/abs/2006.08097                                                                     #
# The pretrained model has been sourced from: https://github.com/yya518/FinBERT                             #
#                                                                                                           #
#                                                                                                           #
# Notice:                                                                                                   #
# This software is intended for demonstration and educational purposes only. This software is               #
# experimental and a work in progress. Under no circumstances should these files be used in                 #
# relation to any critical system(s). Use of these files is at your own risk.                               #
#                                                                                                           #
# Before using it for web scraping any website, always consult that website's terms of use.                 #
# Do not use this software to fetch any data from any website that has forbidden use of web                 #
# scraping or similar mechanisms, or violates its terms of use in any other way. The author is              #
# not liable for such kind of inappropriate use of this software.                                           #
#                                                                                                           #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,                       #
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR                  #
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE                 #
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR                      #
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER                    #
# DEALINGS IN THE SOFTWARE.                                                                                 #
#                                                                                                           #
# ###########################################################################################################

from __future__ import print_function, division

# import standard python libraries:
import logging
import os
from datetime import datetime

# import this project's python libraries:
from base_plugin import BasePlugin
from data_structs import Types
from news_event import NewsEvent

# nlp libraries:
# import nltk
# from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import sent_tokenize

import pandas as pd
import numpy as np
import openpyxl

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import lr_scheduler
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from pytorch_pretrained_bert import BertTokenizer, BertModel, BertConfig

##########

logger = logging.getLogger(__name__)

###########


class mod_eventclass(BasePlugin):
    """ Web Scraping plugin: mod_eventclass
    For classifying news events.
    """
    minArticleLengthInChars = 400
    pluginType = Types.MODULE_DATA_PROCESSOR  # implies data post-processor

    dataFrame = None
    device = None
    model = None
    sentencesColList = ['url', 'sentence', 'sentence_no', 'neutral_prob', 'positive_prob', 'negative_prob']
    sentencesRec = None

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
        self.pretuned_modelfile = self.app_config.checkAndSanitizeConfigString('plugins', 'mod_eventclass_modelfile')
        self.model_weights_path = self.app_config.checkAndSanitizeConfigString('plugins', 'mod_eventclass_weightspath')
        self.vocab_path = self.app_config.checkAndSanitizeConfigString('plugins', 'mod_eventclass_vocab_path')
        self.labels = {0:'neutral', 1:'positive',2:'negative'}
        # TODO: fix model load error:
        self.setupModel()
        self.sentencesRec = pd.DataFrame(
            np.zeros((1, len(self.sentencesColList)), dtype=np.unicode_), columns=self.sentencesColList)
        # convert last 4 into float32 dtype
        for colname in ["sentence_no", "neutral_prob", "positive_prob", "negative_prob"]:
            self.sentencesRec[colname] = pd.to_numeric(self.sentencesRec[colname])

    def setupModel(self):
        """ Load the classification model.
        """
        num_labels= len(self.labels)
        vocab_type = "finance-uncased"
        self.max_seq_length=256
        if torch.cuda.is_available():
            self.device = torch.device("cuda")
        else:
            self.device = torch.device("cpu")
        self.model = BertClassification(
            weight_path=self.model_weights_path,
            num_labels=num_labels,
            vocab=vocab_type)
        self.model.load_state_dict(
            torch.load(self.pretuned_modelfile,
                       map_location=self.device))
        self.model.to(self.device)
        self.tokenizer = BertTokenizer(
            vocab_file=self.vocab_path,
            do_lower_case=True,
            do_basic_tokenize=True)

    def processDataObj(self, newsEventObj):
        """ Process given data object by this plugin.

        :param newsEventObj: The NewsEvent object to be classified.
        :type newsEventObj: NewsEvent
        """
        runDate = datetime.strptime(newsEventObj.getPublishDate(), '%Y-%m-%d')
        logger.info(f"Started news event classification for data in: {newsEventObj.getFileName()}")
        classificationObj = self.classifyText(newsEventObj.getText(), newsEventObj.getURL())
        # put classification field in NewsEvent document:
        newsEventObj.setClassification(classificationObj)
        # prepare filename:
        fileNameWOExt = newsEventObj.getFileName().replace('.json','')
        # save document to file:
        newsEventObj.writeFiles(fileNameWOExt, '', saveHTMLFile=False)
        logger.info(f"Wrote news event classification for data in: {fileNameWOExt} as: {classificationObj}")

    def classifyText(self, textValue, url):
        """
        Examine and classify the text from the document and return classification scores text.

        :param textValue: Text to be examined and classified.
        :type textValue: str
        :return: Classification scores
        :rtype: dict{str:float}
        """
        sentenceDF = None
        classificationScores = {'positive': 0.0, 'neutral': 0.0, 'negative': 0.0}
        try:
            logger.debug(f'Classifying using finbert model for text of length {len(textValue)}')
            if len(textValue) > self.minArticleLengthInChars:
                thisRec = self.sentencesRec.copy(deep=True)
                thisRec['url']=url
                sentences = sent_tokenize(textValue.lower())
                self.model.eval()
                for index, sent in enumerate(sentences):
                    thisRec['sentence']=sent
                    thisRec['sentence_no']=index
                    # apply model on the sentence to get classification scores
                    [neutralProb, positiveProb, negativeProb] = self.classifySentences(sent)
                    thisRec['neutral_prob']=neutralProb
                    thisRec['positive_prob']=positiveProb
                    thisRec['negative_prob']=negativeProb
                    if sentenceDF is None:
                        sentenceDF = thisRec
                    else:
                        sentenceDF = sentenceDF.append(thisRec)
                aggscores = sentenceDF.groupby('url').agg({
                    'neutral_prob':'sum',
                    'positive_prob':'sum',
                    'negative_prob':'sum'})
                classificationScores = {'positive': aggscores['positive_prob'][0],
                                        'neutral': aggscores['neutral_prob'][0],
                                        'negative': aggscores['negative_prob'][0]
                                        }
        except Exception as e:
            print("Error getting sentence classification:", e)
        return(classificationScores)

    def classifySentences(self, sent):
        """ Classify one text sentence at a time.
        """
        tokenized_sent = self.tokenizer.tokenize(sent)
        if len(tokenized_sent) > self.max_seq_length:
            tokenized_sent = tokenized_sent[:self.max_seq_length]
        ids_review  = self.tokenizer.convert_tokens_to_ids(tokenized_sent)
        mask_input = [1]*len(ids_review)
        padding = [0] * (self.max_seq_length - len(ids_review))
        ids_review += padding
        mask_input += padding
        input_type = [0]*self.max_seq_length
        input_ids = torch.tensor(ids_review).to(self.device).reshape(-1, 256)
        attention_mask =  torch.tensor(mask_input).to(self.device).reshape(-1, 256)
        token_type_ids = torch.tensor(input_type).to(self.device).reshape(-1, 256)
        with torch.set_grad_enabled(False):
            outputs = self.model(input_ids, token_type_ids, attention_mask)
            outputs = F.softmax(outputs,dim=1)
            #print('\n FinBERT predicted sentiment: ', labels[torch.argmax(outputs).item()])
            return([i.item() for i in outputs.data[0]])


######


class BertClassification(nn.Module):

    def __init__(self, weight_path, num_labels=2, vocab="finance-uncased"):
        super(BertClassification, self).__init__()
        self.num_labels = num_labels
        self.vocab = vocab

        self.bert = BertModel.from_pretrained(weight_path)
        self.config = BertConfig(vocab_size_or_config_json_file=30873, hidden_size=768, num_hidden_layers=12, num_attention_heads=12, intermediate_size=3072)

        self.dropout = nn.Dropout(self.config.hidden_dropout_prob)
        self.classifier = nn.Linear(self.config.hidden_size, num_labels)
        nn.init.xavier_normal_(self.classifier.weight)

    def forward(self, input_ids, token_type_ids=None, attention_mask=None, labels=None, graphEmbeddings=None):
        _, pooled_output = self.bert(input_ids, token_type_ids, attention_mask)
        pooled_output = self.dropout(pooled_output)
        logits = self.classifier(pooled_output)
        return logits

class dense_opt():
    def __init__(self, model):
        super(dense_opt, self).__init__()
        self.lrlast = .001
        self.lrmain = .00001
        self.optim = optim.Adam(
            [ {"params":model.bert.parameters(),"lr": self.lrmain},
              {"params":model.classifier.parameters(), "lr": self.lrlast},
              ])

    def get_optim(self):
        return self.optim

# # end of file ##
