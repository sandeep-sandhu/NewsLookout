#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_network.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Test for the network class for the web scraping and news text processing application
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
import sys
import os
from datetime import datetime
import pytest

from . import getAppFolders, getMockAppInstance #, list_all_files, read_bz2html_file


# ###################################

# from http import server
# from io import BytesIO as IO
# class HTTPHandler(server.BaseHTTPRequestHandler):
#     """Custom handler"""
#     def do_GET(self):
#         self.send_response(200)
#         self.send_header("Content-type", "text/html")
#         self.end_headers()
#         # return test string as body:
#         html = "<html><p>Goodbye world!</p></html>"
#         self.wfile.write(html.encode('UTF-8'))


@pytest.fixture()
def app_inst(tmpdir):
    """Connect to db before tests, disconnect after."""
    # Setup : start app
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    app_inst = getMockAppInstance(parentFolder,
                                  '2021-06-10',
                                  config_file)

    yield
    # Teardown : stop app
    # delete the log file.


def test_fetchRawDataFromURL():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    app_inst = getMockAppInstance(parentFolder,
                                  '2021-06-10',
                                  config_file)
    import network
    allowedDomains = ['google.com']
    netw_inst = network.NetworkFetcher(app_inst.app_config, allowedDomains)
    uRLtoFetch = 'http://google.com'
    returnResult = netw_inst.fetchRawDataFromURL(uRLtoFetch, 'plugin1', getBytes=False)
    print(f'Size of data fetched from {uRLtoFetch} :\n{len(returnResult)}')
    assert len(returnResult) > 1024, 'Network class is not fetching sufficient data.'


def test_sleepBeforeNextFetch():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    import network
    startTime = datetime.now()
    network.NetworkFetcher.sleepBeforeNextFetch()
    endTime = datetime.now()
    print(f'Start Time: {startTime}, End time = {endTime}')
    time_diff_sec = (endTime - startTime).seconds
    print(f'Time difference 1: {time_diff_sec}')
    assert time_diff_sec >= 6, 'Network sleepBeforeNextFetch() is not correctly waiting upto minimum time delay.'
    assert time_diff_sec <= 10, 'Network sleepBeforeNextFetch() is not correctly waiting till maximum time delay.'
    startTime = datetime.now()
    network.NetworkFetcher.sleepBeforeNextFetch(fix_sec=1, min_rand_sec=2, max_rand_sec=4)
    endTime = datetime.now()
    time_diff_sec = (endTime - startTime).seconds
    print(f'Time difference 2: {time_diff_sec}')
    assert time_diff_sec >= 3, 'Network sleepBeforeNextFetch() is not correctly waiting upto minimum time delay.'
    assert time_diff_sec <= 5, 'Network sleepBeforeNextFetch() is not correctly waiting till maximum time delay.'


if __name__ == "__main__":
    test_sleepBeforeNextFetch()


# end of file
