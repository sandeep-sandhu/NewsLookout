#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
 File name: network.py
 Application: The NewsLookout Web Scraping Application
 Date: 2021-01-14
 Purpose: Network helper class that performs all network operations for all plugins for the application
 Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com

 DISCLAIMER: This software is intended for demonstration and educational purposes only.
 Before using it for web scraping any website, always consult that website's terms of use.
 Do not use this software to fetch any data from any website that has forbidden use of web
 scraping or similar mechanisms, or violates its terms of use in any other way. The author is
 not responsible for such kind of inappropriate use of this software.

"""

##########

# import standard python libraries:
import time
import random
import functools
import logging

# import web retrieval python libraries:
import http
import requests
from urllib3.exceptions import InsecureRequestWarning
import newspaper

# import this project's python libraries:

##########

# setup logging
logger = logging.getLogger(__name__)


class NetworkFetcher:
    """ The network manager class performs all the network processing for the application
    """

    userAgentStrList = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari'
       ]
    userAgentIndex = 0

    fetch_timeout = 60
    connect_timeout = 5
    retryCount = 2
    retryWaitFixed = 37
    retry_wait_rand_max_sec = 17
    retry_wait_rand_min_sec = 1

    proxies = {}
    customHeader = dict()

    cookieJar = None
    newspaper_config = None

    def __init__(self, configData):
        """ Read and apply the configuration data passed by the main application """

        self.configData = configData

        try:
            logger.debug("Configuring the network manager")

            self.retryCount = self.configData['retry_count']
            self.retryWaitFixed = self.configData['retry_wait_sec']
            self.retry_wait_rand_max_sec = int(self.configData['retry_wait_rand_max_sec'])
            self.retry_wait_rand_min_sec = int(self.configData['retry_wait_rand_min_sec'])

            self.proxies = self.configData['proxies']
            self.proxy_ca_certfile = self.configData['proxy_ca_certfile']

            self.newspaper_config = self.configData['newspaper_config']

            self.fetch_timeout = self.configData['fetch_timeout']
            self.connect_timeout = self.configData['connect_timeout']

        except Exception as e:
            logger.error("Exception when configuring the network manager: %s", e)

        try:
            # Apply the configuration

            # this is a pipe separated list of user-agent strings to be used in a round robin manner
            self.userAgentStrList = self.configData['user_agent'].split('|')

            # Suppress only the single warning from urllib3 for not verifying SSL certificates
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

            self.customHeader = {'user-agent': self.userAgentStrList[0]}

        except Exception as e:
            logger.error("Exception when configuring the network manager: %s", e)

    def NewsPpr_get_html_2XX_only(url, config=None, response=None):
        """ Replacement for method: newspaper.network.get_html_2XX_only()
        Consolidated logic for http requests from newspaper. We handle error cases:
        - Attempt to find encoding of the html by using HTTP header. Fallback to 'ISO-8859-1' if not provided.
        - Error out if a non 2XX HTTP response code is returned.
        """

        logger.debug("Newspaper library retrieving URL: %s", url)
        config = config or newspaper.network.Configuration()
        useragent = config.browser_user_agent
        timeout = config.request_timeout
        proxies = config.proxies
        headers = config.headers

        if response is not None:
            return newspaper.network._get_html_from_response(response)

        response = requests.get(
            url=url,
            **newspaper.network.get_request_kwargs(timeout, useragent, proxies, headers),
            verify=False)

        html = newspaper.network._get_html_from_response(response)
        if config.http_success_only:
            # fail if HTTP sends a non 2XX response
            response.raise_for_status()

        return html

    def sleepBeforeNextFetch(self):
        """ Sleep random time before the next HTTP(S) fetch
        """
        pauseTime = self.retryWaitFixed + random.randint(
            self.retry_wait_rand_min_sec, self.retry_wait_rand_max_sec)

        logger.debug("Pausing web retrieval for %s seconds.", pauseTime)
        time.sleep(pauseTime)

#    @functools.lru_cache(maxsize=512)
    @functools.lru_cache(maxsize=2048)
    def fetchRawDataFromURL(self, uRLtoFetch, pluginName):
        """ fetching raw content From given URL
        """
        rawDataContent = b""

        if len(uRLtoFetch) > 11:

            for retryCounter in range(self.retryCount):

                logger.debug("RetryCounter %s: Downloading Raw Data for URL %s",
                             retryCounter, uRLtoFetch.encode('ascii', "ignore"))

                try:
                    self.customHeader = {'user-agent': self.userAgentStrList[self.userAgentIndex]}

                    # self.requestOpener = urllib3.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookieJar))

                    # response = self.requestOpener.open("http://example.com/")

                    httpsRequests = requests.get(
                        uRLtoFetch,
                        headers=self.customHeader,
                        timeout=(self.connect_timeout, self.fetch_timeout),
                        proxies=self.proxies
                        # verify=False,                    # disables checking SSL/proxy certs

                        # verify=self.proxy_ca_certfile   # provides CA cert

                        )
                    rawDataContent = httpsRequests.content
                    # all done without error, so don't retry again.
                    break

                except requests.Timeout as timeoutExp:
                    logger.error("%s: Request timeout (retry count = %s) downloading raw data From URL %s: %s",
                                 pluginName,
                                 retryCounter,
                                 uRLtoFetch,
                                 timeoutExp)

                except requests.ConnectionError as connExp:
                    logger.error("%s: Connection error (retry count = %s) downloading raw data From URL %s: %s",
                                 pluginName,
                                 retryCounter,
                                 uRLtoFetch,
                                 connExp)

                except requests.HTTPError as httpExp:
                    logger.error("%s: HTTP error (retry count = %s) downloading raw data From URL %s: %s",
                                 pluginName,
                                 retryCounter,
                                 uRLtoFetch,
                                 httpExp)

                except requests.RequestException as reqExp:
                    logger.error("%s: Ambiguous request error (retry count = %s) downloading raw data From URL %s: %s",
                                 pluginName,
                                 retryCounter,
                                 uRLtoFetch,
                                 reqExp)

#                 except requests.URLRequired , requests.TooManyRedirects
#                     break;# stop retrying again for this error

                except Exception as e:
                    logger.error("%s: Stopping the download, general error (retry count = %s) for URL %s Error: %s",
                                 pluginName,
                                 retryCounter,
                                 uRLtoFetch,
                                 e)
                    break  # stop retrying again for this error

                finally:
                    if (self.userAgentIndex + 1) == len(self.userAgentStrList):
                        self.userAgentIndex = 0
                    else:
                        self.userAgentIndex = self.userAgentIndex + 1

                    self.sleepBeforeNextFetch()

        return(rawDataContent)

    def loadAndSetCookies(self, cookieFileName):
        """ load and Set Cookies from file
        """
        cookieJar = None
        try:
            cookieJar = http.cookiejar.FileCookieJar(cookieFileName)

        except Exception as theError:
            logger.error("Exception caught opening cookie file: %s", theError)

        return(cookieJar)

    def getCookiePolicy(self, listOfAllowedDomains):
        """ """
        thisCookiePolicy = None

        try:
            thisCookiePolicy = http.cookiejar.DefaultCookiePolicy(
                blocked_domains=None,
                allowed_domains=listOfAllowedDomains,
                netscape=True,
                rfc2965=False,
                rfc2109_as_netscape=None,
                hide_cookie2=False,
                strict_domain=False,
                strict_rfc2965_unverifiable=True,
                strict_ns_unverifiable=False,
                strict_ns_domain=http.cookiejar.DefaultCookiePolicy.DomainLiberal,
                strict_ns_set_initial_dollar=False,
                strict_ns_set_path=False)

            thisCookiePolicy.set_allowed_domains(listOfAllowedDomains)

        except Exception as e:
            logger.error("Error setting cookie policy: %s", e)

        return(thisCookiePolicy)

# # end of file ##
