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

testfolder = os.path.dirname(os.path.realpath(__file__))
parentFolder = os.path.dirname(testfolder)
sourceFolder = os.path.join(parentFolder, 'src')
sys.path.append(sourceFolder)
sys.path.append(os.path.join(sourceFolder, 'plugins'))
sys.path.append(os.path.join(sourceFolder, 'plugins_contrib'))

os.chdir(parentFolder)

__version__ = '1.9.9'

# end of file #
