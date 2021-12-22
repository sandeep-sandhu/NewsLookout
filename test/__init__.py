#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File: Test modules for NewsLookout application
 Application: The NewsLookout Web Scraping Application
 Date: 2021-06-10
 Purpose: Test module for the web scraping and news text processing application
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
# import standard python libraries:
import sys
import os

def getAppFolders():
    testfolder = os.path.dirname(os.path.realpath(__file__))
    parentFolder = os.path.dirname(testfolder)
    sourceFolder = os.path.join(parentFolder, 'newslookout')
    sys.path.append(sourceFolder)
    sys.path.append(os.path.join(sourceFolder, 'plugins'))
    sys.path.append(os.path.join(sourceFolder, 'plugins_contrib'))
    testdataFolder = os.path.join(parentFolder, 'test-data')
    return((parentFolder, sourceFolder, testdataFolder))


def getMockAppInstance(parentFolder, rundate, configfile):
    from scraper_app import NewsLookout
    mock_sys_argv = ['python.exe', '-c', configfile,
                     '-d', rundate]
    # instantiate the main application class
    local_app_inst = NewsLookout()
    local_app_inst.config(mock_sys_argv)
    return(local_app_inst)


def list_all_files(directoryName):
    if os.path.isdir(directoryName) is True:
        filesList = [os.path.join(directoryName, i) for i in os.listdir(directoryName)
                     if os.path.isfile(os.path.join(directoryName, i))]
        return(filesList)


def altfetchRawDataFromURL(feedFileName, pluginName):
    with open(feedFileName, 'rt', encoding='utf-8') as fp:
        file_contents = fp.read()
        fp.close()
        return(file_contents)


def read_bz2html_file(filename: str) -> str:
    """ Reads contents from BZ2 compressed HTML file.

    :param filename: BZ2 archive to read
    :return: HTML content read from file
    """
    import bz2
    with bz2.open(filename, "rb") as f:
        # Decompress data from file
        content = f.read()
    return(content.decode('UTF-8'))


def get_network_substitute_fun(plugin_name: str, testdata_dir: str, file_no: int = 0) -> object:
    files_list = list_all_files(testdata_dir)
    listofFiles = [i for i in files_list if i.find(plugin_name) >= 0 and i.find('.bz2')> 0]
    if file_no < len(listofFiles):
        htmlBz2FileName = listofFiles[file_no]
    else:
        htmlBz2FileName = listofFiles[0]
    print(f'Generating data supplying function to get data from file #{file_no}')
    def replacement_fun(uRLtoFetch, pluginName, getBytes=False):
        html_content = read_bz2html_file(htmlBz2FileName)
        print(f'Read {len(html_content)} characters of HTML content from file: {htmlBz2FileName}')
        return(html_content)
    return(replacement_fun)


(parentFolder, sourceFolder, testdataFolder) = getAppFolders()
os.chdir(parentFolder)

__version__ = '2.0.0'

# end of file #
