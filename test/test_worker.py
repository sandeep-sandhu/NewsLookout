#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: test_worker.py
 Application: The NewsLookout Web Scraping Application
 Date: 2020-01-11
 Purpose: Test for the worker class for the web scraping and news text processing application
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
import threading

from newslookout.data_structs import PluginTypes
from . import getAppFolders, getMockAppInstance, list_all_files, read_bz2html_file
from newslookout import scraper_utils

# ###################################

global app_inst

def test_worker_init():
    # Test PluginWorker object init.
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    global app_inst
    app_inst = getMockAppInstance(parentFolder,
                                  '2021-06-10',
                                  config_file)
    app_inst.queue_manager.config(app_inst.app_config)
    from newslookout.plugins.mod_en_in_inexp_business import mod_en_in_inexp_business
    import newslookout.data_structs
    import newslookout.session_hist
    pluginInst = mod_en_in_inexp_business()
    dbAccessSem = threading.Semaphore()
    sessionHistoryDB = newslookout.session_hist.SessionHistory(':memory:', dbAccessSem)
    from newslookout.worker import PluginWorker, ProgressWatcher, DataProcessor
    workerInst = PluginWorker(pluginInst,
                              PluginTypes.TASK_GET_URL_LIST,
                              sessionHistoryDB,
                              app_inst.queue_manager)
    assert type(workerInst) == PluginWorker, 'Worker object is not initialising correctly'
    print(f'Queue Fill wait time = {workerInst.queueFillwaitTime}')
    assert workerInst.queueFillwaitTime == 120, 'Worker object is not initialising queue wait time correctly'
    workerInst.setRunDate(app_inst.app_config.rundate)
    assert workerInst.runDate == app_inst.app_config.rundate, 'Worker object is unable to set rundate correctly'
    # test runURLListGatherTasks()
    def patch_fun(paramA, paramB):
        return ['https://www.newindianexpress.com/news1',
                'https://www.newindianexpress.com/news2',
                'https://www.newindianexpress.com/news3']
    # patch mock function for getURLsListForDate():
    pluginInst.getURLsListForDate = patch_fun
    workerInst.runURLListGatherTasks()
    assert pluginInst.getQueueSize()>0, 'runURLListGatherTasks()  is not retrieving URLs correctly.'
    while pluginInst.getQueueSize()>0:
        print(f'Queue size: {pluginInst.getQueueSize()}')
        print(f'Next item in queue: {pluginInst.getNextItemFromFetchQueue()}')

    # test news aggregator URL sourcing logic:
    test_dom_plugin_map = {'www.newindianexpress.com':'plugin1', 'www.thehindu.com':'plugin2'}
    allPluginObjs = {'plugin2': b'objectbytes', 'plugin1':b'objectotherbytes'}
    workerInst.setDomainMapAndPlugins(test_dom_plugin_map, allPluginObjs)
    assert workerInst.domainToPluginMap == test_dom_plugin_map,\
        'Worker object is unable to set domainToPluginMap correctly'
    assert workerInst.pluginNameToObjMap == allPluginObjs, \
        'Worker object is unable to set pluginNameToObjMap correctly'
    urlList = ['https://www.newindianexpress.com/news1',
               'https://www.newindianexpress.com/news2',
               'https://www.newindianexpress.com/news3',
               'https://www.thehindu.com/news4',
               'https://www.thehindu.com/news5']
    plugin_to_url_list_map = workerInst.aggregator_url2domain_map(urlList, allPluginObjs, test_dom_plugin_map)
    print(f'plugin_to_url_list_map = {plugin_to_url_list_map}, length = {len(plugin_to_url_list_map)}')


def test_ProgressWatcher_init():
    # TODO: implement this
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    global app_inst
    app_inst = getMockAppInstance(parentFolder,
                                  '2021-06-10',
                                  config_file)
    app_inst.queue_manager.config(app_inst.app_config)
    from newslookout.plugins.mod_en_in_inexp_business import mod_en_in_inexp_business
    import newslookout.data_structs
    import newslookout.session_hist
    import newslookout.queue_manager
    pluginInst = mod_en_in_inexp_business()
    allPluginObjsMap = {'plugin2': b'objectbytes', 'plugin1':b'objectotherbytes'}
    dbAccessSem = threading.Semaphore()
    sessionHistoryDB = newslookout.session_hist.SessionHistory(':memory:', dbAccessSem)
    queue_status = newslookout.queue_manager.QueueStatus(app_inst.queue_manager)
    from newslookout.worker import PluginWorker, ProgressWatcher, DataProcessor
    workerInst = ProgressWatcher(allPluginObjsMap,
                                 sessionHistoryDB,
                                 app_inst.queue_manager,
                                 queue_status,
                                 app_inst.app_config,
                                 name='55',
                                 daemon=False)
    assert type(workerInst) == ProgressWatcher, 'ProgressWatcher object is not initialising correctly'


def test_DataProcessor_processItem_skips_already_processed():
    """DataProcessor should not re-process URLs already in alreadyDataProcList."""
    (parentFolder, sourceFolder, testdataFolder, config_file) = getAppFolders()
    global app_inst
    app_inst = getMockAppInstance(parentFolder, '2021-06-10', config_file)
    app_inst.queue_manager.config(app_inst.app_config)
    from newslookout.worker import DataProcessor
    from newslookout.data_structs import ExecutionResult
    import threading

    # Build a minimal execution result
    exec_result = ExecutionResult(
        'https://example.com/article/1', 1000, 500, '2021-06-10',
        'test_plugin', dataFileName='/data/test_plugin_1', success=True
    )
    # Pre-populate alreadyDataProcList to simulate duplication
    app_inst.queue_manager.alreadyDataProcList = [exec_result.URL]
    # processItem should add to processed queue without calling plugin
    called = []
    class FakePlugin:
        pluginName = 'fake'
        def loadDocument(self, f): called.append(f); return None
    DataProcessor.processItem(
        app_inst.queue_manager,
        exec_result,
        [],
        {},
        'worker-1'
    )
    assert called == [], 'processItem must not call loadDocument for already-processed URLs'

if __name__ == "__main__":
    test_worker_init()

# end of file
