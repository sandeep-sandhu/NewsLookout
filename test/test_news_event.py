#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_news_event.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Test for the main class for the web scraping and news text processing application
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

# ###################################


# import standard python libraries:
import datetime
import sys
import os
from . import getAppFolders, getMockAppInstance, list_all_files, read_bz2html_file


# ###################################


def test_setClassification():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    thisObj = news_event.NewsEvent()
    classificationScores = {'positive': 0.90, 'neutral': 0.10, 'negative': 0.25}
    thisObj.setClassification(classificationScores)
    assert thisObj.getClassification() == classificationScores, \
        'setClassification() is not setting the event class probabilities of the news object correctly.'


def test_setModuleName():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    thisObj = news_event.NewsEvent()
    thisObj.setModuleName('mod_myplugin_zyx')
    assert thisObj.getModuleName() == 'mod_myplugin_zyx', 'setModuleName() is not working correctly'


def test_setTriggerWordFlag():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    thisObj = news_event.NewsEvent()
    thisObj.setTriggerWordFlag('key1', 0)
    thisObj.setTriggerWordFlag('key2', 1)
    trigDictionary = thisObj.getTriggerWords()
    assert trigDictionary['key1'] == 0, 'setTriggerWordFlag() is not working correctly'
    assert trigDictionary['key2'] == 1, 'setTriggerWordFlag() is not working correctly'


def test_toJSON():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    import json
    thisObj = news_event.NewsEvent()
    thisObj.setText('The article text')
    thisObj.setTitle('The Title')
    thisObj.setURL('https://www.news.com/today/stories')
    jsonDataStr = thisObj.toJSON()
    convertedObj = json.loads(jsonDataStr)
    assert convertedObj['title'] == 'The Title', 'toJSON() saving title is not working correctly'
    assert convertedObj['text'] == 'The article text', 'toJSON() saving article content is not working correctly'
    assert convertedObj['URL'] == 'https://www.news.com/today/stories', 'toJSON() URL save is not working correctly'


def test_setIndustries():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    thisObj = news_event.NewsEvent()
    articleIndustryList = ['Auto', 'BFSI']
    thisObj.setIndustries(articleIndustryList)
    assert thisObj.urlData["industries"] == ['Auto', 'BFSI'], 'setIndustries() is not working correctly'


def test_setArticleID():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    thisObj = news_event.NewsEvent()
    uniqueID = '102847593'
    thisObj.setArticleID(uniqueID)
    assert thisObj.getArticleID() == uniqueID, 'setArticleID() is not working correctly'


def test_readFromJSON():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    thisObj = news_event.NewsEvent()
    # get json from test-data folder
    jsonFilename = os.path.join(testdataFolder, 'test_readFromJSON.json')
    thisObj.readFromJSON(jsonFilename)
    print(f'Read the data of length {len(thisObj.urlData)} from test json file')
    assert thisObj.getPublishDate() == datetime.datetime.strptime("2019-12-23",'%Y-%m-%d'), 'readFromJSON() is not working correctly, incorrect pub date'
    assert thisObj.urlData["title"] == "Explained: What is the Citizenship Amendment Bill?",\
        'readFromJSON() is not working correctly, title not read correctly.'

def test_cleanText():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    testData = ' This is SOME dirty text.â€™🙂â™\t '
    resultData = news_event.NewsEvent.cleanText(testData)
    print(f'Input text was: "{testData}", Output text is: "{resultData}"')
    assert resultData == 'This is SOME dirty text.', 'cleanText() is not working correctly'
    testData = '“Double Quotes”'
    resultData = news_event.NewsEvent.cleanText(testData)
    print(f'Input text was: {testData}, Output text is: {resultData}')
    assert news_event.NewsEvent.cleanText(testData) == '\'Double Quotes\'',\
        'cleanText() is not working correctly: not cleaning special character double quotes'
    assert news_event.NewsEvent.cleanText('‘Quotes’') == '\'Quotes\'', \
        'cleanText() is not working correctly: not cleaning special character quotes'
    assert news_event.NewsEvent.cleanText('–Hyphens—') == '-Hyphens-', \
        'cleanText() is not working correctly: not cleaning special character hyphens'

def test_writeFiles():
    # TODO: implement this
    assert 1==1, 'writeFiles() is not working correctly'

def test_importNewspaperArticleData():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import news_event
    thisObj = news_event.NewsEvent()
    from newspaper import Article
    newspaperArticle = Article('https://somesite.com/news/todaysarticle.html')
    # newspaperArticle.url = 'https://somesite.com/news/todaysarticle.html'
    newspaperArticle.text = 'This is the First sentence in the text. This is the second one.'
    newspaperArticle.authors = ['Author 1', 'Agency 2']
    newspaperArticle.publish_date = datetime.datetime.strptime('2021-01-31', '%Y-%m-%d')
    newspaperArticle.title = "This News Article's Title!"
    newspaperArticle.html = '<html><head><title>Title 1</title></head><body>Heading 1</body></html>'
    newspaperArticle.keywords = ['First KeyWord', 'Second KeyWord']
    newspaperArticle.meta_data['keywords'] = "keyw1,keyw2"
    newspaperArticle.meta_data['news_keywords'] = "keyw3,keyw4"
    thisObj.importNewspaperArticleData(newspaperArticle)
    print(f"Imported article data as:\n{thisObj.urlData}")
    assert thisObj.getText() == 'This is the First sentence in the text. This is the second one.', \
        'importNewspaperArticleData() is not working correctly; body text not imported!'
    assert thisObj.urlData['title'] == "This News Article's Title!", \
        'importNewspaperArticleData() is not working correctly; News article Title not imported!'
    assert thisObj.getURL() == 'https://somesite.com/news/todaysarticle.html', \
        'importNewspaperArticleData() is not working correctly; published date string not imported!'
    assert thisObj.getPublishDate() == datetime.datetime.strptime('2021-01-31','%Y-%m-%d'), \
        'importNewspaperArticleData() is not working correctly; published date string not imported!'
    assert thisObj.getKeywords() == ['First KeyWord', 'Second KeyWord', 'keyw1', 'keyw2', 'keyw3', 'keyw4'],\
        'importNewspaperArticleData() is not working correctly; keyword not imported'
    assert thisObj.getAuthors() == ['Author 1', 'Agency 2'], \
        'importNewspaperArticleData() is not working correctly; keyword not imported'


if __name__ == "__main__":
    test_importNewspaperArticleData()

# end of file
