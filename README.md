# NewsLookout Web Scraping Application
The NewsLookout web scraping application is intended for gathering financial events from public news websites.
It is a scalable, modular and configurable multi-threaded python console application.
The application is readily extended by adding custom modules via its 'plugin' architecture.
Plugins can be added for a variety of tasks, including - for scraping additional news sources, perform custom data pre-processing and run NLP based news text analytics such as - entity recognition, negative event classification, economy trends, industry trends, etc.


![Build Status](https://github.com/sandeep-sandhu/NewsLookout/actions/workflows/python-app.yml/badge.svg) ![GitHub release](https://img.shields.io/github/v/release/sandeep-sandhu/NewsLookout.svg)

## Features

There already exist a number of python libraries for web-scraping, so why should you consider this application for web scraping news? The reason is that it has been specifically built for sourcing news and has several useful features. A few notable ones are:

- [x] Multi-threaded for scraping several news sites in parallel
- [x] Rigorously tested for the specific websites enabled in the plugins, handles several quirks and formatting problems caused by inconsistent and non-standard HTML code.
- [x] Reduces the network traffic and consequently webserver load by pausing between network requests. High traffic load are usually detected and blocked. The application reduces network traffic to avoid overloading the news web servers.
- [x] Keeps track of failures and history of sites scraped to avoid re-visiting them again
- [x] Completely configurable functionality
- [x] Works with proxy servers
- [x] Enterprise ready functionality - configurable event logging, segregation of data store, etc.
- [x] Runnable without a frontend, as a daemon.
- [x] Extensible with custom plugins that can be rapidly written with minimal additional code to support additional news sources. Writing a new plugin does not need writing low level code to handle network traffic and HTTP protocols.
- [x] Rigorous text cleaning
- [x] Builtin NLP support for keyword extraction and compute document similarity
- [x] Text de-duplication using advanced NLP models
- [x] Extensible data processing plugins to customize the data processing required after web scraping
- [x] Enables web-scraping news archives to get news from previous dates for establishing history for analysis
- [x] Saves present state and resumes unfinished URLs if the application is shut-down midway during web scraping


## Installation
Install the dependencies using pip:
>     pip install -r requirements.txt

Install the application via pip:
>     pip install newslookout

_Caution: As a security best practice, it is strongly recommended to run the application under its own separate Operating System level user ID without any special privileges._

Next, create and configure separate locations for:
- The application itself (not required if you're installing via the wheel or pip installer)
- The data files downloaded from the news websites, e.g. - `/var/cache/newslookout`
- The log file, e.g. - `/var/log/newslookout/newslookout.log`
- The PID file, e.g. - `/var/run/newslookout.pid`

Set these parameters in the configuration file.

## NLP Data

Download the spacy model using this command:
> python -m spacy download en_core_web_lg

For NLTK, download the following data:
  1. reuters
  1. universal_treebanks_v20
  1. maxent_treebank_pos_tagger
  1. punkt

Either use the nltk downloader:

>     import nltk
>     nltk.download()

Or else, manually download these from the source location - https://github.com/nltk/nltk_data/tree/gh-pages/packages

If these are not installed to one of the standard locations, you will need to set the NLTK_DATA environment variable to specify the location of this NLTK data. Refer to the NLTK website on downloading the data - https://www.nltk.org/data.html.


## Configuration

All the parameters for the application can be configured via the configuration file.
The configuration file, and the date for which the web scraper is to be run, are both passed as command line arguments to the application.

The key parameters that need to be configured are:
  1. Application root folder: `prefix`
  1. Data directory: `data_dir`
  1. Plugin directory: `plugins_dir`
  1. Contributed Plugins: `plugins_contributed_dir`
  1. Enabled plugins: Add the name of the python file (without file extension) under the `plugins` section as: `plugin01=mod_my_plugin`
  1. Network proxy (if any): `proxy_url_https`
  1. The level of logging: `log_level`


## Usage

Installing the wheel or via pip will generate a script `newslookout` placed in your local folder.
This invokes the main method of the application and should be passed the two required arguments - configuration file and date for which the application is run.
For example:
>     newslookout -c myconfigfile.conf -d 2020-01-01

In addition to this, 2 scripts are provided for UNIX-like and Windows OS.
For convenience, you may run these shell scripts to start the application, it automatically generates the current date and supplies it as an argument to the python application.
Its best advised to run the scripts or command line by scheduling it via the UNIX cron scheduler or the Microsoft Windows Task Scheduler for automated scheduling for small setups.
In large enterprise environments, batch job coordination software such as Ctrl-M, IBM Tivoli, or any job scheduling framework may be configured to run it for reliable and automated execution.

### PID File
The application creates a PID file (process identifier) upon startup to prevent launching multiple instances at the same time.
On startup, it checks if this file exists, if it does then the application will stop.
If the application is killed or shuts down abruptly without cleaning up, this PID file will remain and will need to be manually deleted.

### Console Display
The application displays its progress on stdout, for example:

>     NewsLookout Web Scraping Application, Version  1.9.0
>     Python version:  3.8.8 (tags/v3.8.8:024d805, Feb 19 2021, 13:18:16) [MSC v.1928 64 bit (AMD64)]
>     Reading configuration settings from file: conf\newslookout.conf
>     Saving data to: data\web_scrapes
>     Logging events to file: temp\newslookout.log
>     Web-scraping Progress:
>      12%|████▍                                 | 680/5786 [18:00<2:15:09,  1.59s/it]


### Event Log
For a more detailed log of events, refer to the log file.
It captures all events with timestamp and the relevant name of the module that generated the event.

>     2021-01-01 01:31:50:[INFO]:queue_manager:4360: 13 worker threads available to fetch content.
>     ...
>     2021-01-01 02:07:51:[INFO]:worker:320: Progress Status: 1117 URLs successfully scraped out of 1334 processed; 702 URLs remain.
>     ...
>     2021-01-01 03:02:10:[INFO]:queue_manager:5700: Completed fetching data on all worker threads

## Customizing and Writing your own Plugins

You can extend the web scraper's functionality to add any additional website that you need scraped by using the template file `template_for_plugin.py` from the `plugins_contrib` folder and customising it.
Name your custom plugin file with the same name as the name of the class object.
Place it in the `plugins_contrib` folder (or whichever folder you have set in the config file).
Next, add the plugin's name in the configuration file.
It will be read, instantiated and run automatically by the application on the next startup.

Take a look at one of the already implemented plugins code for examples of how a plugin can be written.


## Maintenance and Monitoring

### Data Size
The application will automatically rotate the log file upon reaching the set maximum size.
The data directory will need to be monitored since its size could grow quickly due to
 the data scraped from the web.

### Event Monitoring
For enterprise installations, log watch may be enabled for monitoring the operation of
 the application by watching for specific event entries in the log file.

The data folder should be monitored for growth in its size.

### HTML parsing code updates
In case news portals change their structure,
 the web scraper code for their respective plugin will need to be updated to continue 
 retrieving information reliably.
This needs careful monitoring of the output to keep checking for parsing related problems. 
