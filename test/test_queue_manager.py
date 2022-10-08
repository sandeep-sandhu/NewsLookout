#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_queue_manager.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Test for the QueueManager class for the web scraping and news text processing application
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
import queue
import sys
import os
from . import getAppFolders, getMockAppInstance, list_all_files, read_bz2html_file


# ###################################


def test_queue_manager_init_config():
    # Test init() and config():
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    app_inst = getMockAppInstance(parentFolder,
                                  '2021-06-10',
                                  config_file)
    app_inst.app_queue_manager.config(app_inst.app_config)
    assert type(app_inst.app_queue_manager.fetchCompletedQueue) == queue.Queue,\
        'Queue manager: fetchCompletedQueue was not configured correctly.'


if __name__ == "__main__":
    test_queue_manager_init_config()

# end of file
