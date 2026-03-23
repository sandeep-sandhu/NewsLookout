
![Build Status](https://github.com/sandeep-sandhu/NewsLookout/actions/workflows/python-app.yml/badge.svg) ![GitHub release](https://img.shields.io/github/v/release/sandeep-sandhu/NewsLookout.svg) [![Coverage Status](https://coveralls.io/repos/github/sandeep-sandhu/NewsLookout/badge.svg?branch=main)](https://coveralls.io/github/sandeep-sandhu/NewsLookout?branch=main) [![Pypi Release](https://img.shields.io/pypi/v/newslookout.svg?style=flat-square&label=PyPI)](https://pypi.org/project/newslookout/)
[![Python Versions](https://img.shields.io/pypi/pyversions/newslookout.svg?style=flat-square&label=Python%20Versions)](https://pypi.org/project/newslookout/)
[![Contributors](https://img.shields.io/github/contributors/sandeep-sandhu/NewsLookout.svg)](https://github.com/sandeep-sandhu/NewsLookout/contributors)

# NewsLookout Web Scraping Application - Complete Documentation

## Table of Contents
1. [Overview](#overview)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Library Usage](#library-usage)
5. [Architecture](#architecture)
6. [Configuration](#configuration)
7. [Plugin Development](#plugin-development)
8. [API Reference](#api-reference)
9. [Troubleshooting](#troubleshooting)


## Overview

NewsLookout is a comprehensive, multi-threaded web scraping framework designed for extracting news articles and data from various online sources. It features a plugin-based architecture for extensibility and supports concurrent processing across multiple news sources.

### Key Features

- **Multi-threaded Architecture**: Concurrent URL discovery, content fetching, and data processing
- **Plugin-Based Design**: Easy to extend with custom scrapers for different news sources
- **Session Management**: Tracks completed URLs to avoid duplicate processing
- **Data Processing Pipeline**: Built-in support for deduplication, classification, and keyword extraction
- **Graceful Shutdown**: Handles interrupts cleanly without data loss
- **Library Interface**: Can be used as a Python library in your own applications
- **Configurable Timeouts**: Prevents indefinite hangs with configurable timeout mechanisms

### Recent Improvements (v3.0.0)

1. **Timeout Management**: URL gathering operations now have configurable timeouts (default: 10 minutes)
2. **Dedicated Database Thread**: All database operations handled by single thread to prevent lock conflicts
3. **Improved Recursion**: Iterative link extraction with strict depth limiting (max 4 levels)
4. **Better Interrupt Handling**: Graceful shutdown on Ctrl+C with proper cleanup
5. **Queue-Based URL Streaming**: URLs processed as discovered, not in batches
6. **Library Interface**: Can be imported and used programmatically

## Installation

### From PyPI

```bash
pip install newslookout
```

### Directory layout after installation

When installed via pip, NewsLookout stores all user-writable files outside the Python
package directory so that package upgrades never overwrite your data or configuration.

| Platform | Config file | Log / PID files | Data & archive |
|----------|-------------|-----------------|----------------|
| **Linux** | `~/.config/newslookout/newslookout.conf` | `~/.local/state/newslookout/` | `~/.local/share/newslookout/data/` |
| **macOS** | `~/Library/Preferences/newslookout/newslookout.conf` | `~/Library/Logs/newslookout/` | `~/Library/Application Support/newslookout/data/` |
| **Windows** | `%APPDATA%\newslookout\newslookout.conf` | `%APPDATA%\newslookout\` | `%APPDATA%\newslookout\data\` |

> **Tip:** You can override any path in the config file.  Set the `data_dir`, `log_file`,
> and `archive_base_path` keys under `[environment]` to any absolute path you prefer.

### First run

The first time you run `newslookout` without specifying a config file it will:

1. Create the default configuration at the platform-appropriate path shown above.
2. Print the path and exit so you can review it before scraping begins.

```bash
newslookout          # first run: creates config and exits
# Edit the config, then:
newslookout -d 2024-03-22
```

You can also point to a custom config explicitly:

```bash
newslookout -c /path/to/my.conf -d 2024-03-22
```


### From Source

```bash
git clone https://github.com/sandeep-sandhu/newslookout.git
cd newslookout
pip install -e .
```

### Dependencies

NewsLookout requires Python 3.8+ and will install the following dependencies:

- `beautifulsoup4` - HTML parsing
- `newspaper3k` - Article extraction
- `nltk` - Natural language processing
- `requests` - HTTP requests
- `pandas` - Data manipulation
- `enlighten` - Progress bars
- `spacy` - Advanced NLP (optional, for deduplication)
- `torch` - Deep learning (optional, for classification)


### NLP Data Model Dependencies

After installation, download the required NLP model data:

```bash
# spaCy model (required for deduplication plugin)
python -m spacy download en_core_web_lg

# NLTK data (required for keyword extraction)
python - <<'EOF'
import nltk
for pkg in ['punkt', 'punkt_tab', 'maxent_treebank_pos_tagger',
            'reuters', 'universal_treebanks_v20']:
    nltk.download(pkg)
EOF
```

If NLTK data is stored in a non-standard location, set the `NLTK_DATA` environment variable
to its path. See https://www.nltk.org/data.html for details.


Alternatively, you could manually download these from the source location - https://github.com/nltk/nltk_data/tree/gh-pages/packages

For NLTK, refer to the NLTK website on downloading the data - https://www.nltk.org/data.html.
Specifically, the following data needs to be downloaded:
1. reuters
1. universal_treebanks_v20
1. maxent_treebank_pos_tagger
1. punkt



## Quick Start

### Command Line Usage

```bash
# Basic usage
newslookout -c config.conf -d 2025-12-21

# With verbose logging
newslookout -c config.conf -d 2025-12-21 --log-level DEBUG
```

### Library Usage

```python
from newslookout import NewsLookoutApp

# Create and run the application
app = NewsLookoutApp(config_file='config.conf')
stats = app.run(run_date='2025-12-21', max_runtime=3600)

print(f"Processed {stats['urls_processed']} URLs in {stats['duration']:.1f} seconds")
```

### Using Context Manager

```python
from newslookout import NewsLookoutApp

with NewsLookoutApp('config.conf') as app:
    app.start()  # Run in background
    # Do other work...
    app.stop()
```

### Quick Scrape Function

```python
from newslookout import scrape

# One-line scraping
stats = scrape('config.conf', run_date='2025-12-21', max_runtime=3600)
```

## Library Usage

### Basic Example

```python
from newslookout import NewsLookoutApp

# Initialize
app = NewsLookoutApp(config_file='path/to/config.conf')

# Run synchronously
stats = app.run(run_date='2025-12-21')

print(f"URLs discovered: {stats['urls_discovered']}")
print(f"URLs processed: {stats['urls_processed']}")
print(f"Data processed: {stats['data_processed']}")
print(f"Duration: {stats['duration']:.1f} seconds")
```

### Background Execution

```python
from newslookout import NewsLookoutApp
import time

app = NewsLookoutApp('config.conf')

# Start in background
app.start()

# Monitor progress
while app.is_running:
    stats = app.get_statistics()
    print(f"Progress: {stats['urls_processed']} URLs processed")
    time.sleep(10)

# Wait for completion
app.wait_for_completion()

# Get final statistics
final_stats = app.get_statistics()
```

### With Timeout

```python
from newslookout import NewsLookoutApp

app = NewsLookoutApp('config.conf')

# Run for maximum 1 hour
stats = app.run(max_runtime=3600)

if app.is_running:
    print("Timeout reached, stopping...")
    app.stop()
```

### Getting Plugin Status

```python
app = NewsLookoutApp('config.conf')
app.start()

# Check plugin status
plugin_status = app.get_plugin_status()
for plugin_name, state in plugin_status.items():
    print(f"{plugin_name}: {state}")
```

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────┐
│                  NewsLookoutApp                      │
│              (Library Interface)                     │
└───────────────────┬─────────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────────┐
│                 QueueManager                         │
│          (Orchestrates all workers)                  │
└─────┬────────────┬────────────┬────────────┬────────┘
      │            │            │            │
      ▼            ▼            ▼            ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│   URL    │ │ Content  │ │   Data   │ │ Progress │
│Discovery │ │ Fetching │ │Processing│ │ Watcher  │
│ Workers  │ │ Workers  │ │ Workers  │ │          │
└──────────┘ └──────────┘ └──────────┘ └──────────┘
      │            │            │            │
      └────────────┴────────────┴────────────┘
                    │
            ┌───────▼────────┐
            │   Database     │
            │     Worker     │
            │  (Dedicated)   │
            └────────────────┘
```

### Thread Model

1. **URL Discovery Workers**: One per plugin, discovers URLs to scrape
2. **Content Fetch Workers**: Multiple workers that download and parse content
3. **Data Processing Workers**: Process scraped data through plugins
4. **Database Worker**: Single thread handling all database operations
5. **Progress Watcher**: Monitors progress and updates UI

### Queue System

- **URL Discovery Queue**: New URLs streamed here as discovered
- **Fetch Queue**: URLs pending content download
- **Processing Queue**: Downloaded content pending processing
- **Database Queue**: Database operations to be executed
- **Completed Queue**: Finished items

## Configuration

### Configuration File Structure

```ini
[installation]
prefix = /opt/newslookout
data_dir = /var/cache/newslookout_data
plugins_dir = /opt/newslookout/plugins
log_file = /var/log/newslookout/app.log
pid_file = /tmp/newslookout.pid

[operation]
# URL gathering timeout (seconds)
url_gathering_timeout = 600

# Recursion level for link extraction (1-4)
recursion_level = 2

# Network settings
user_agent = Mozilla/5.0 ...
fetch_timeout = 60
connect_timeout = 3
retry_count = 3

# Proxy settings (optional)
proxy_url_http = http://proxy.example.com:8080
proxy_url_https = https://proxy.example.com:8080

[logging]
log_level = INFO
max_logfile_size = 10485760
logfile_backup_count = 30

[plugins]
plugin1 = mod_en_in_ecotimes|10
plugin2 = mod_en_in_timesofindia|20
plugin3 = mod_dedupe|100
```

### Configuration Parameters

#### URL Gathering
- `url_gathering_timeout`: Maximum seconds for URL discovery (default: 600)
- `recursion_level`: Depth of link extraction (1-4, default: 2)

#### Network
- `fetch_timeout`: Timeout for downloading content (seconds)
- `connect_timeout`: Timeout for establishing connection (seconds)
- `retry_count`: Number of retry attempts
- `user_agent`: User agent string for requests

#### Database
- `completed_urls_datafile`: SQLite database for session history

#### Logging
- `log_level`: DEBUG, INFO, WARNING, ERROR
- `max_logfile_size`: Maximum log file size before rotation
- `logfile_backup_count`: Number of rotated logs to keep

## Plugin Development

### Creating a News Scraper Plugin

```python
from base_plugin import BasePlugin
from data_structs import PluginTypes

class mod_my_news_site(BasePlugin):
    """
    Plugin for scraping MyNewsSite.com
    """
    
    # Plugin configuration
    pluginType = PluginTypes.MODULE_NEWS_CONTENT
    mainURL = 'https://www.mynewssite.com'
    allowedDomains = ['www.mynewssite.com']
    
    # URL patterns
    validURLStringsToCheck = ['mynewssite.com/article/']
    invalidURLSubStrings = ['mynewssite.com/ads/', '/video/']
    
    # Required methods
    def __init__(self):
        super().__init__()
    
    def getURLsListForDate(self, runDate, sessionHistoryDB):
        """Discover URLs for given date."""
        urls = []
        # Your URL discovery logic
        return urls
    
    def extractArticleBody(self, htmlContent):
        """Extract article text from HTML."""
        # Your extraction logic
        return text
    
    def extractUniqueIDFromURL(self, url):
        """Extract unique identifier from URL."""
        # Your ID extraction logic
        return unique_id
```

### Creating a Data Processor Plugin

```python
from base_plugin import BasePlugin
from data_structs import PluginTypes

class mod_my_processor(BasePlugin):
    """
    Plugin for processing scraped data
    """
    
    pluginType = PluginTypes.MODULE_DATA_PROCESSOR
    
    def __init__(self):
        super().__init__()
    
    def additionalConfig(self, sessionHistoryObj):
        """Additional configuration."""
        pass
    
    def processDataObj(self, newsEventObj):
        """Process a news event object."""
        # Your processing logic
        newsEventObj.setText(processed_text)
        
        # Save changes
        filename = newsEventObj.getFileName().replace('.json', '')
        newsEventObj.writeFiles(filename, '', saveHTMLFile=False)
```

### Plugin Types

- `MODULE_NEWS_CONTENT`: Scrapes news articles
- `MODULE_NEWS_AGGREGATOR`: Aggregates URLs from multiple sources
- `MODULE_DATA_CONTENT`: Scrapes structured data
- `MODULE_DATA_PROCESSOR`: Post-processes scraped data

## API Reference

### NewsLookoutApp Class

#### Constructor

```python
NewsLookoutApp(config_file: str, run_date: Optional[str] = None)
```

**Parameters:**
- `config_file` (str): Path to configuration file
- `run_date` (str, optional): Date in 'YYYY-MM-DD' format

**Raises:**
- `FileNotFoundError`: If config file doesn't exist
- `ValueError`: If configuration is invalid

#### Methods

##### run()

```python
run(run_date: Optional[str] = None, 
    max_runtime: Optional[int] = None,
    blocking: bool = True) -> Dict[str, Any]
```

Run the scraping process.

**Parameters:**
- `run_date` (str, optional): Override run date
- `max_runtime` (int, optional): Maximum runtime in seconds
- `blocking` (bool): If True, wait for completion

**Returns:**
- `dict`: Statistics dictionary

##### start()

```python
start()
```

Start application in background mode.

##### stop()

```python
stop(timeout: int = 30)
```

Stop the running application gracefully.

**Parameters:**
- `timeout` (int): Maximum seconds to wait for shutdown

##### get_statistics()

```python
get_statistics() -> Dict[str, Any]
```

Get current or last run statistics.

**Returns:**
- `dict`: Statistics including:
  - `urls_discovered`: Total URLs found
  - `urls_processed`: URLs successfully scraped
  - `data_processed`: Items processed
  - `start_time`: Execution start time
  - `end_time`: Execution end time
  - `duration`: Runtime in seconds
  - `is_running`: Current status

##### get_plugin_status()

```python
get_plugin_status() -> Dict[str, str]
```

Get status of all loaded plugins.

**Returns:**
- `dict`: Map of plugin names to states

##### wait_for_completion()

```python
wait_for_completion(timeout: Optional[int] = None) -> bool
```

Wait for background execution to complete.

**Parameters:**
- `timeout` (int, optional): Maximum seconds to wait

**Returns:**
- `bool`: True if completed, False if timeout

### Convenience Functions

#### scrape()

```python
scrape(config_file: str, 
       run_date: Optional[str] = None,
       max_runtime: Optional[int] = None) -> Dict[str, Any]
```

Convenience function to run a scraping job.

## Troubleshooting

### Common Issues

#### 1. URL Gathering Timeout

**Symptom:** Application hangs during URL discovery

**Solution:** Increase `url_gathering_timeout` in configuration:

```ini
[operation]
url_gathering_timeout = 1200  # 20 minutes
```

#### 2. Database Lock Errors

**Symptom:** `database is locked` errors in logs

**Solution:** All database operations now go through dedicated thread. If issue persists:
- Check no other process is accessing the database
- Remove `-journal` files if present
- Increase timeout in session_hist.py

#### 3. Keyboard Interrupt Not Working

**Symptom:** Ctrl+C doesn't stop the application

**Solution:** Updated code includes periodic shutdown checks. Ensure:
- Using latest version
- Not stuck in long-running external call
- Check network timeouts are reasonable

#### 4. Too Many URLs

**Symptom:** Memory exhaustion from excessive URLs

**Solution:**
- Reduce `recursion_level` in configuration
- Improve URL filtering in plugins
- Use more restrictive `validURLStringsToCheck`

#### 5. Plugin Hanging

**Symptom:** Specific plugin never completes

**Solution:**
- Check plugin's `is_stopped` flag periodically
- Ensure network operations have timeouts
- Review `getURLsListForDate()` implementation

### Debug Mode

Enable detailed logging:

```ini
[logging]
log_level = DEBUG
```

Or programmatically:

```python
import logging
logging.getLogger('').setLevel(logging.DEBUG)
```

### Performance Tuning

#### Optimize Network Operations

```ini
[operation]
fetch_timeout = 30  # Reduce if sites are fast
retry_count = 2     # Reduce retries
```

#### Adjust Worker Counts

Modify in code:

```python
queue_manager.dataproc_threads = 10  # Increase for more parallelism
```

#### Control Recursion

```ini
[operation]
recursion_level = 1  # Minimum recursion
```

## Best Practices

### 1. Configuration Management

- Use separate configs for different environments
- Version control your configuration files
- Document custom settings

### 2. Plugin Development

- Always check `self.is_stopped` in loops
- Use timeouts for all network operations
- Handle exceptions gracefully
- Log progress at regular intervals

### 3. Resource Management

- Monitor disk space for data directory
- Rotate logs regularly
- Clean up old session data periodically

### 4. Production Deployment

- Use systemd or supervisor for service management
- Set up log rotation
- Monitor application health
- Configure appropriate timeouts
- Use separate database for each instance

### 5. Error Handling

- Review logs after each run
- Set up alerts for critical errors
- Test plugins with edge cases
- Handle malformed HTML gracefully

### Common log errors and fixes

| Log message | Cause | Fix |
|-------------|-------|-----|
| `can't compare offset-naive and offset-aware datetimes` | The news site returns a timezone-aware publication date | Apply Patch 2 to `base_plugin.py` |
| `'NoneType' object has no attribute 'getURL'` in `mod_keywordflags` | The JSON article file for a previously scraped URL no longer exists on disk | Apply Patch 4 to `worker.py`; also verify your `data_dir` path in the config |
| `Invalid article_id: None` / `Falling back to legacy file storage` | The URL did not match any `urlMatchPatterns` in the plugin | Apply Patch 3 to `base_plugin.py` |
| `Error fetching status: TypeError: can't access property "textContent" … is null` | Dashboard JS runs before DOM is ready | Apply Patch 5 to `dashboard.html` |
| `Request for font "Ubuntu Sans" blocked at visibility level 2` | Browser privacy policy blocks Google Fonts | Apply Patch 5a to `dashboard.html` |
| Installed package appears under `src/newslookout` instead of `newslookout` | Missing `src`-layout config in `setup.cfg` / `pyproject.toml` | Apply Patches 9 and 10 |


## Support and Contributing

- **Documentation**: https://github.com/sandeep-sandhu/newslookout
- **Issues**: Report bugs on GitHub Issues
- **Contributing**: Pull requests welcome

## License

This software is provided "AS IS" without warranty. See LICENSE file for details.

---

