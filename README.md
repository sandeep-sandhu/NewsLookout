# NewsLookout Web Scraping Application
The NewsLookout web scraping application is intended for gathering financial events from public news websites.
It is a scalable, modular and configurable multi-threaded python console application.
The application is readily extended by adding custom modules via its 'plugin' architecture. Plugins can be added for a variety of tasks including - for scraping additional news sources, perform custom data pre-processing and run NLP based news text analytics such as - entity recognition, negative event classification, economy trends, industry trends, etc.


![Build Status](https://github.com/sandeep-sandhu/NewsLookout/actions/workflows/python-app.yml/badge.svg)


## Installation
Create separate directory locations for:
  - the application itself,
  - the data files downloaded from the news websites, and
  - the log file
  - the PID file


Install the dependencies using pip:
>         pip install -r requirements.txt

As a security best practice, its advisable to run the application under its own separate Operating System level user ID.

## NLP Data

Download the spacy model using this command:
>         python -m spacy download en_core_web_lg

For NLTK, download the following data:
  1. reuters
  1. universal_treebanks_v20
  1. maxent_treebank_pos_tagger
  1. punkt

Either use the nltk downloader:
>         import nltk
>         nltk.download()

Or else, manually download these from the source location - https://github.com/nltk/nltk_data/tree/gh-pages/packages

If these are not installed to one of the standard locations, you will need to set the NLTK_DATA environment variable to specify the location of this NLTK data.


## Configuration

All the parameters for the application can be configured via the configuration file.
The configuration file and the date for which the web scraper is to be run, are both passed as command line arguments to the application.

The key parameters that need to be configured are:
  1. Application root folder
  1. Data directory
  1. Plugin directory
  1. Enabled plugins
  1. Network parameters such as proxy, retry timeout, etc.
  1. The level of recursion
  1. The level of logging


## Usage

Run the shell script for unix-like operating systems, or use the windows batch command script under windows.

The scripts may be invoked by scheduling it via the UNIX cron scheduler or the Microsoft Windows Task Scheduler for automated scheduling.

The application created a PID file upon startup and checks for the existence of this file at startup.  The application will not stop.

The application displays its progress on stdout, for example:

>         NewsLookout Web Scraping Application, Version  1.9.0
>         Python version:  3.8.8 (tags/v3.8.8:024d805, Feb 19 2021, 13:18:16) [MSC v.1928 64 bit (AMD64)]
>         Reading configuration settings from file: conf\newslookout.conf
>         Saving data to: data\data
>         Logging events to file: temp\newslookout.log
>         Web-scraping Progress:
>          12%|████▍                                 | 680/5786 [18:00<2:15:09,  1.59s/it]


For a more detailed log of events, refer to the log file.


## Maintenance and Monitoring

The application will automatically rotate the log file upon reaching the set maximum size.
The data directory will need to be monitored since its size could grow quickly due to
 the data scraped from the web.
For enterprise installations, log watch may be enabled for monitoring the operation of
 the application by watching for specific event entries in the log file. The data folder should be monitored for growth in its size.


In case news portals change their structure,
 the web scraper code for their respective plugin will need to be updated to continue 
 retrieving information reliably.

