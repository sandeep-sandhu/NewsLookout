#!/usr/bin/env python
# -*- coding: utf-8 -*-

# #########################################################################################################
#                                                                                                         #
# File name: network.py                                                                                   #
# Application: The NewsLookout Web Scraping Application                                                   #
# Date: 2021-06-23                                                                                        #
# Purpose: Network helper class that performs all network operations for all plugins for the application  #
# Copyright 2021, The NewsLookout Web Scraping Application, Sandeep Singh Sandhu, sandeep.sandhu@gmx.com  #
#                                                                                                         #
#                                                                                                         #
# Notice:                                                                                                 #
# This software is intended for demonstration and educational purposes only. This software is             #
# experimental and a work in progress. Under no circumstances should these files be used in               #
# relation to any critical system(s). Use of these files is at your own risk.                             #
#                                                                                                         #
# Before using it for web scraping any website, always consult that website's terms of use.               #
# Do not use this software to fetch any data from any website that has forbidden use of web               #
# scraping or similar mechanisms, or violates its terms of use in any other way. The author is            #
# not liable for such kind of inappropriate use of this software.                                         #
#                                                                                                         #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,                     #
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR                #
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE               #
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR                    #
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER                  #
# DEALINGS IN THE SOFTWARE.                                                                               #
#                                                                                                         #
# #########################################################################################################


# import standard python libraries:
import time
import random
import functools
import logging

# import web retrieval python libraries:
import http
import ssl
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.util import ssl_  # For legacy SSL
from urllib3.exceptions import InsecureRequestWarning
import newspaper

from newslookout import scraper_utils

##########

# setup logging
logger = logging.getLogger(__name__)


class HTTPError:
    """Container for HTTP error information."""

    def __init__(self, status_code: int, url: str, message: str = None):
        self.status_code = status_code
        self.url = url
        self.message = message or f"HTTP {status_code}"
        # Don't retry these error codes
        self.is_permanent = status_code in [400, 401, 403, 404, 405, 410, 451]

    def __str__(self):
        return f"HTTP {self.status_code}: {self.message}"


class LegacySSLAdapter(HTTPAdapter):
    """Adapter to allow legacy SSL/TLS versions."""
    def init_poolmanager(self, connections, maxsize, block=False):
        context = ssl_.create_urllib3_context(ciphers='DEFAULT@SECLEVEL=1')
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       ssl_context=context)


class NetworkFetcher:
    """ The network manager class performs all the network processing for the application
    """
    userAgentStrList = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari'
       ]
    userAgentIndex = 0
    app_config = None
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

    def __init__(self, app_config, allowedDomains):
        """ Read and apply the configuration data passed by the main application
        """
        self.app_config = app_config
        try:
            logger.debug("Configuring the network manager")
            self.retryCount = self.app_config.retry_count
            self.retryWaitFixed = self.app_config.retry_wait_sec
            self.retry_wait_rand_max_sec = int(self.app_config.retry_wait_rand_max_sec)
            self.retry_wait_rand_min_sec = int(self.app_config.retry_wait_rand_min_sec)
            self.proxies = self.app_config.proxies
            self.proxy_ca_certfile = self.app_config.proxy_ca_certfile
            self.verify_ca_cert = self.app_config.verify_ca_cert
            self.newspaper_config = self.app_config.newspaper_config
            self.fetch_timeout = self.app_config.fetch_timeout
            self.connect_timeout = self.app_config.connect_timeout
        except Exception as e:
            logger.error("Exception when configuring the network manager: %s", e)
        # Apply the configuration:
        try:
            # this is a pipe separated list of user-agent strings to be used in a round robin manner
            self.userAgentStrList = self.app_config.user_agent.split('|')
            # Suppress only the single warning from urllib3 for not verifying SSL certificates
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

            # Initialize a Session object
            self.session = requests.Session()
            self.session.headers.update({'user-agent': self.userAgentStrList[0]})

            # Mount legacy adapter for specific problematic domains if needed, or globally
            legacy_adapter = LegacySSLAdapter()
            self.session.mount('https://', legacy_adapter)

            # Cookies setup (simplified)
            self.cookieJar = self.loadAndSetCookies(self.app_config.cookie_file)
            if self.cookieJar:
                self.session.cookies.update(self.cookieJar)

        except Exception as e:
            logger.error("Exception when configuring the network manager: %s", e)

    @staticmethod
    @functools.lru_cache(maxsize=100)
    def NewsPpr_get_html_2XX_only(url: str, config=None, response=None):
        """ Replacement for method: newspaper.network.get_html_2XX_only()
        Consolidated logic for http requests from newspaper. Handles error cases:
        - Attempt to find encoding of the html by using HTTP header. Fallback to 'ISO-8859-1' if not provided.
        - Error out if a non 2XX HTTP response code is returned.

        :param url: URL to fetch
        :param config: newspaper.config object with HTTP protocol request options such as proxy, timeouts, etc.
        :param response: HTTP Response object to extract data from.
        :return:
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
            verify=False,
            **newspaper.network.get_request_kwargs(timeout, useragent, proxies, headers)
            )
        html = newspaper.network._get_html_from_response(response)
        if config.http_success_only:
            # fail if HTTP sends a non 2XX response
            response.raise_for_status()
        return html

    @staticmethod
    def sleepBeforeNextFetch(fix_sec: int = 3,
                             min_rand_sec: int = 3,
                             max_rand_sec: int = 7,
                             shutdown_event=None):
        """Sleep with shutdown checking.
        Sleep for a random time period before the next HTTP(S) fetch.
        In addition to a fixed time period, a random integer is generated
         which has a range of min_rand_sec to max_rand_sec, this is added to the fixed time period.

        :param fix_sec: Fixed time period in seconds to wait for (default = 3)
        :param min_rand_sec: Minimum time period in seconds for the random additional wait time (default = 3)
        :param max_rand_sec: Maximum time period in seconds for the random additional wait time (default = 7)
        :return:
        """
        pause_time_seconds = fix_sec + random.randint(min_rand_sec, max_rand_sec)
        logger.debug("Pausing web retrieval for %s seconds.", pause_time_seconds)

        # Sleep in 1-second intervals to check shutdown
        for _ in range(pause_time_seconds):
            if shutdown_event and shutdown_event.is_set():
                logger.debug("Sleep interrupted by shutdown signal")
                return
            time.sleep(1)

    def fetchRawDataFromURL_with_error_handling(self, uRLtoFetch: str, pluginName: str,
                                                getBytes: bool = False, shutdown_event=None):
        """
        Fetch raw HTML content from URL with proper HTTP error handling and shutdown checks.

        This is the enhanced version that should be added to NetworkFetcher class.

        Args:
            uRLtoFetch (str): URL to fetch
            pluginName (str): Plugin name for logging
            getBytes (bool): Return bytes instead of string

        Returns:
            tuple: (content, http_error) where http_error is HTTPError or None
        """
        httpsResponse = None
        http_error = None

        if not uRLtoFetch or len(uRLtoFetch) < 11:
            return None, None

        for retryCounter in range(self.retryCount):
            # Check shutdown before each retry
            if shutdown_event and shutdown_event.is_set():
                logger.info(f"{pluginName}: Fetch cancelled due to shutdown")
                return None, None

            logger.debug("RetryCounter %s: Downloading Raw Data for URL %s",
                         retryCounter, uRLtoFetch.encode('ascii', "ignore"))
            try:
                # Rotate User Agent
                ua = self.userAgentStrList[self.userAgentIndex]
                self.session.headers.update({'user-agent': ua})

                # Use the session
                httpsResponse = self.session.get(
                    uRLtoFetch,
                    timeout=(self.connect_timeout, self.fetch_timeout),
                    proxies=self.proxies,
                    verify=False
                )

                # CHECK FOR HTTP ERRORS
                if httpsResponse.status_code >= 400:
                    http_error = HTTPError(httpsResponse.status_code, uRLtoFetch)

                    if http_error.is_permanent:
                        logger.warning(f"{pluginName}: Permanent HTTP {httpsResponse.status_code}: {uRLtoFetch}")
                        return None, http_error

                    httpsResponse.raise_for_status()

                break  # Success

            except requests.HTTPError as httpExp:
                if httpExp.response:
                    http_error = HTTPError(httpExp.response.status_code, uRLtoFetch, str(httpExp))
                    if http_error.is_permanent:
                        return None, http_error

            except (requests.Timeout, requests.ConnectionError) as e:
                logger.error(f"{pluginName}: Network error (retry {retryCounter}): {e}")
                if shutdown_event and shutdown_event.is_set():
                    return None, None
                if isinstance(e, requests.ConnectionError):
                    break   # stop retrying on connection errors

            except requests.TooManyRedirects as httpExp:
                logger.error(
                    f"{pluginName}: Too Many Redirects (retry count = {retryCounter}) for URL {uRLtoFetch}: {httpExp}")
                if httpExp.response:
                    http_error = HTTPError(httpExp.response.status_code, uRLtoFetch, str(httpExp))
                    return None, http_error
                break

            except requests.URLRequired as httpExp:
                logger.error(f"{pluginName}: URLRequired (retry count = {retryCounter}) for URL {uRLtoFetch}: {httpExp}")
                if httpExp.response:
                    http_error = HTTPError(httpExp.response.status_code, uRLtoFetch, str(httpExp))
                    return None, http_error
                break

            except requests.RequestException as reqExp:
                logger.error(f"{pluginName}: Request error (retry count = {retryCounter}) for URL {uRLtoFetch}: {reqExp}")

            except Exception as e:
                logger.error(f"{pluginName}: General error (retry count = {retryCounter}) for URL {uRLtoFetch}: {e}")
                break

            finally:
                self.userAgentIndex = (self.userAgentIndex + 1) % len(self.userAgentStrList)
                if retryCounter < self.retryCount - 1:
                    # Check shutdown during sleep
                    if shutdown_event and shutdown_event.is_set():
                        return None, None
                    NetworkFetcher.sleepBeforeNextFetch(
                        self.retryWaitFixed,
                        self.retry_wait_rand_min_sec,
                        self.retry_wait_rand_max_sec,
                        shutdown_event=shutdown_event
                    )

        content = self.getDataFromHTTPResponse(httpsResponse, getBytes) if httpsResponse else None
        return content, http_error

    def fetchRawDataFromURL(self, uRLtoFetch: str, pluginName: str, getBytes: bool = False, shutdown_event=None):
        """
        Fetch raw HTML content with HTTP error tracking and shutdown support.

        Returns:
            tuple: (content, http_error) where http_error is HTTPError object or None
        """
        return self.fetchRawDataFromURL_with_error_handling(uRLtoFetch, pluginName, getBytes, shutdown_event)

    def getDataFromHTTPResponse(self,
                                httpsResponse: requests.Response,
                                getBytes: bool) -> str:
        """ Get data From HTTP response.

        :param httpsResponse:
        :param getBytes:
        :return: str
        """
        if httpsResponse is not None and httpsResponse.encoding != 'ISO-8859-1' and getBytes is False:
            return httpsResponse.text
        elif httpsResponse is not None and getBytes is False:
            htmlText = httpsResponse.content
            if 'charset' not in httpsResponse.headers.get('content-type', ''):
                encodings = requests.utils.get_encodings_from_content(httpsResponse.text)
                if len(encodings) > 0:
                    httpsResponse.encoding = encodings[0]
                    htmlText = httpsResponse.text
            return htmlText
        elif httpsResponse is not None:
            return httpsResponse.content.decode(encoding="utf-8", errors="ignore")
        else:
            return None

    def loadAndSetCookies(self, cookieFileName: str) -> object:
        """ Load and Set Cookies from text file

        :param cookieFileName: Text file to read the previously saved cookies
        :return: CookieJar instantiated with cookie data form file.
        """
        cookieJar = None
        try:
            cookieJar = http.cookiejar.FileCookieJar(cookieFileName)
        except Exception as theError:
            logger.error("Exception caught opening cookie file: %s", theError)
        return cookieJar

    def getCookiePolicy(self, listOfAllowedDomains: list) -> object:
        """ Prepare a cookie jar policy to be used in HTTP requests and sessions.

        :param listOfAllowedDomains: List of domain names permitted for this plugin.
        :return: CookiePolicy
        """
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
            logger.error("Error preparing cookie policy: %s", e)
        return thisCookiePolicy

    def getHTTPData(self,
                    uRLtoFetch: str,
                    postHeaders: dict = None,
                    pluginName: str = None) -> requests.Response:
        """Fetch data using HTTP(s) GET Method, send back response object.
        Uses custom agent, proxy and timeouts configured for the network Fetcher object

        :param uRLtoFetch: URL to fetch
        :param postHeaders: Dictionary of key-value pairs to set custom headers in the request
        :param pluginName: Name of the plugin
        :return: HTTP Response object
        """
        httpsResponse = None
        for retryCounter in range(self.retryCount):
            logger.debug("RetryCounter %s: Posting HTTP content for URL %s",
                         retryCounter, uRLtoFetch.encode('ascii', "ignore"))
            try:
                self.customHeader = {'user-agent': self.userAgentStrList[self.userAgentIndex]}
                if postHeaders is None:
                    postHeaders = self.customHeader
                else:
                    postHeaders.update(self.customHeader)
                httpsResponse = requests.get(
                    uRLtoFetch,
                    headers=postHeaders,
                    timeout=(self.connect_timeout, self.fetch_timeout),
                    proxies=self.proxies,
                    verify=self.verify_ca_cert  # warning: false disables checking SSL certs!
                    )
                break  # completed without error, so don't retry again.
            except Exception as e:
                logger.error(f"{pluginName}: Stopping the download, general error (retry count = {retryCounter})" +
                             f" for http GET on URL {uRLtoFetch} Error: {e}")
                break  # stop retrying again for this error
            finally:
                if (self.userAgentIndex + 1) == len(self.userAgentStrList):
                    self.userAgentIndex = 0
                else:
                    self.userAgentIndex = self.userAgentIndex + 1
                # wait for a random time period
                NetworkFetcher.sleepBeforeNextFetch(fix_sec=self.retryWaitFixed,
                                                    min_rand_sec=self.retry_wait_rand_min_sec,
                                                    max_rand_sec=self.retry_wait_rand_max_sec)
        return httpsResponse

    def postHTTPData(self, uRLtoFetch: str,
                     payload: str,
                     jsonBody: str = None,
                     postHeaders: dict = None,
                     pluginName: str = None) -> bytes:
        """ Fetch data content by POSTing HTTP request to the given URL.

        :param uRLtoFetch: URL to fetch
        :param postHeaders: Dictionary of key-value pairs to set custom headers in the request
        :param payload:
        :param jsonBody:
        :param pluginName: Name of of the plugin
        :return: Content (in bytes) extracted from the HTTP Response.
        """
        rawDataContent = b""
        for retryCounter in range(self.retryCount):
            logger.debug("RetryCounter %s: Posting HTTP content for URL %s",
                         retryCounter, uRLtoFetch.encode('ascii', "ignore"))
            try:
                self.customHeader = {'user-agent': self.userAgentStrList[self.userAgentIndex]}
                if postHeaders is None:
                    postHeaders = self.customHeader
                else:
                    postHeaders.update(self.customHeader)
                httpsResponse = requests.post(
                    uRLtoFetch,
                    data=payload,
                    json=jsonBody,
                    headers=postHeaders,
                    timeout=(self.connect_timeout, self.fetch_timeout),
                    proxies=self.proxies,
                    verify=self.verify_ca_cert  # warning: false disables checking SSL certs!
                    )
                rawDataContent = httpsResponse.content
                break  # all done without error, so don't retry again.
            except Exception as e:
                logger.error(f"{pluginName}: Stopping download, general error (retry count = {retryCounter})" +
                             f" on http POST URL {uRLtoFetch}; Error: {e}")
                break  # stop retrying again for this error
            finally:
                if (self.userAgentIndex + 1) == len(self.userAgentStrList):
                    self.userAgentIndex = 0
                else:
                    self.userAgentIndex = self.userAgentIndex + 1
                # wait for a random time period
                NetworkFetcher.sleepBeforeNextFetch(fix_sec=self.retryWaitFixed,
                                                    min_rand_sec=self.retry_wait_rand_min_sec,
                                                    max_rand_sec=self.retry_wait_rand_max_sec)
        return rawDataContent

    def getDataInSession(self, urlList):
        """ Open a single https session and read several urls """
        pass


# # end of file ##
