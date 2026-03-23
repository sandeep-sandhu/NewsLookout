#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Archive Writer Module for NewsLookout Web Scraping Application
Thread-safe writer for managing daily .zip archives containing articles and raw HTML

Uses Python's built-in zipfile module for maximum compatibility and reliability.
"""

import os
import logging
import threading
import datetime
from pathlib import Path
from typing import Optional, Tuple
import zipfile
from io import BytesIO

logger = logging.getLogger(__name__)


class ArchiveWriter:
    """
    Thread-safe writer for managing daily .zip archives.
    Each date gets its own archive file, with proper locking for concurrent access.

    Uses Python's built-in zipfile module for in-memory operations.
    """

    def __init__(self, base_archive_path: str):
        """
        Initialize the archive writer.

        :param base_archive_path: Base directory where archives will be stored
        """
        self.base_archive_path = Path(base_archive_path)
        self.base_archive_path.mkdir(parents=True, exist_ok=True)

        # Lock dictionary: one lock per archive file (per date)
        self._locks = {}
        self._locks_lock = threading.Lock()  # Lock for the locks dictionary itself

        logger.info(f"ArchiveWriter initialized with base path: {self.base_archive_path}")

    def _get_archive_lock(self, archive_path: str) -> threading.Lock:
        """
        Get or create a lock for a specific archive file.

        :param archive_path: Path to the archive file
        :return: Threading lock for this archive
        """
        with self._locks_lock:
            if archive_path not in self._locks:
                self._locks[archive_path] = threading.Lock()
            return self._locks[archive_path]

    def _get_archive_path(self, date: datetime.date) -> Path:
        """
        Get the archive file path for a given date.

        :param date: Date for the archive
        :return: Path object for the archive file
        """
        # Create subdirectory structure: YYYY/
        year_dir = self.base_archive_path / str(date.year)
        year_dir.mkdir(parents=True, exist_ok=True)

        # Archive filename: YYYY-MM-DD.zip
        archive_filename = f"{date.year}-{date.month:02d}-{date.day:02d}.zip"
        return year_dir / archive_filename

    def write_article(self,
                      json_content: str,
                      raw_html_content: bytes,
                      article_id: str,
                      publish_date: datetime.date,
                      plugin_name: str) -> Tuple[str, str]:
        """
        Write an article (JSON + raw HTML) to the appropriate daily archive.

        :param json_content: Article data in JSON format (string)
        :param raw_html_content: Raw HTML content (bytes, uncompressed or bz2 compressed)
        :param article_id: Unique identifier for the article
        :param publish_date: Publication date of the article
        :param plugin_name: Name of the plugin that scraped this article
        :return: Tuple of (archive_path, article_id)
        """
        # Validate article_id
        if article_id is None or str(article_id).strip() == '' or str(article_id) == 'None':
            raise ValueError(f"Invalid article_id: {article_id}")

        archive_path = self._get_archive_path(publish_date)
        archive_lock = self._get_archive_lock(str(archive_path))

        # Internal archive paths - flat structure
        # Structure: article_id.json and article_id.html
        json_internal_path = f"{article_id}.json"
        html_internal_path = f"{article_id}.html"

        # Decompress HTML if it's bz2 compressed (avoid double compression)
        try:
            if isinstance(raw_html_content, bytes) and raw_html_content.startswith(b'BZ'):
                import bz2
                raw_html_content = bz2.decompress(raw_html_content)
                logger.debug(f"Decompressed bz2 HTML for article {article_id}")
        except Exception as e:
            logger.warning(f"Failed to decompress bz2 HTML, storing as-is: {e}")

        # Ensure raw_html_content is bytes
        if isinstance(raw_html_content, str):
            raw_html_content = raw_html_content.encode('utf-8')

        with archive_lock:
            try:
                # Read existing archive into memory if it exists
                existing_data = {}
                if archive_path.exists():
                    try:
                        with zipfile.ZipFile(archive_path, 'r') as archive:
                            for name in archive.namelist():
                                existing_data[name] = archive.read(name)
                        logger.debug(f"Read {len(existing_data)} existing files from {archive_path}")
                    except zipfile.BadZipFile as e:
                        logger.error(f"Corrupted archive {archive_path}: {e}")
                        # Create backup of corrupted archive
                        backup_path = archive_path.with_suffix('.zip.backup')
                        archive_path.rename(backup_path)
                        logger.warning(f"Corrupted archive moved to {backup_path}")
                        existing_data = {}
                    except Exception as e:
                        logger.error(f"Error reading existing archive {archive_path}: {e}")
                        existing_data = {}

                # Add new files to the data
                existing_data[json_internal_path] = json_content.encode('utf-8')
                existing_data[html_internal_path] = raw_html_content

                # Write everything to a temporary file first (atomic operation)
                temp_archive_path = archive_path.with_suffix('.zip.tmp')

                with zipfile.ZipFile(temp_archive_path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                    for internal_path, content in existing_data.items():
                        archive.writestr(internal_path, content)

                # Atomically replace old archive with new one
                if archive_path.exists():
                    archive_path.unlink()
                temp_archive_path.rename(archive_path)

                logger.debug(f"Successfully wrote article {article_id} to archive {archive_path}")
                return str(archive_path), article_id

            except Exception as e:
                logger.error(f"Error writing to archive {archive_path}: {e}", exc_info=True)
                # Clean up temp file if it exists
                temp_archive_path = archive_path.with_suffix('.zip.tmp')
                if temp_archive_path.exists():
                    try:
                        temp_archive_path.unlink()
                    except:
                        pass
                raise

    def read_article(self,
                     publish_date: datetime.date,
                     plugin_name: str,
                     article_id: str) -> Optional[Tuple[str, bytes]]:
        """
        Read an article from the archive.

        :param publish_date: Publication date of the article
        :param plugin_name: Name of the plugin (unused in flat structure, kept for API compatibility)
        :param article_id: Article identifier
        :return: Tuple of (json_content, raw_html_content) or None if not found
        """
        archive_path = self._get_archive_path(publish_date)

        if not archive_path.exists():
            logger.warning(f"Archive not found: {archive_path}")
            return None

        json_internal_path = f"{article_id}.json"
        html_internal_path = f"{article_id}.html"

        archive_lock = self._get_archive_lock(str(archive_path))

        with archive_lock:
            try:
                with zipfile.ZipFile(archive_path, 'r') as archive:
                    # Check if files exist in archive
                    all_names = archive.namelist()
                    if json_internal_path not in all_names or html_internal_path not in all_names:
                        logger.warning(f"Article {article_id} not found in archive {archive_path}")
                        return None

                    # Read the files
                    json_content = archive.read(json_internal_path).decode('utf-8')
                    html_content = archive.read(html_internal_path)

                    return json_content, html_content

            except zipfile.BadZipFile as e:
                logger.error(f"Corrupted archive {archive_path}: {e}")
                return None
            except Exception as e:
                logger.error(f"Error reading from archive {archive_path}: {e}", exc_info=True)
                return None

    def list_articles(self, publish_date: datetime.date) -> list:
        """
        List all articles in an archive for a given date.

        :param publish_date: Date to query
        :return: List of article_ids (strings)
        """
        archive_path = self._get_archive_path(publish_date)

        if not archive_path.exists():
            return []

        archive_lock = self._get_archive_lock(str(archive_path))

        with archive_lock:
            try:
                with zipfile.ZipFile(archive_path, 'r') as archive:
                    file_list = archive.namelist()

                    articles = set()
                    for file_path in file_list:
                        if file_path.endswith('.json'):
                            # Extract article_id from article_id.json
                            article_id = file_path.rsplit('.', 1)[0]
                            articles.add(article_id)

                    return sorted(list(articles))

            except Exception as e:
                logger.error(f"Error listing archive {archive_path}: {e}", exc_info=True)
                return []

    def get_archive_stats(self, publish_date: datetime.date) -> Optional[dict]:
        """
        Get statistics about an archive.

        :param publish_date: Date to query
        :return: Dictionary with archive statistics or None
        """
        archive_path = self._get_archive_path(publish_date)

        if not archive_path.exists():
            return None

        try:
            stat = archive_path.stat()
            article_count = len(self.list_articles(publish_date))

            return {
                'archive_path': str(archive_path),
                'size_bytes': stat.st_size,
                'size_mb': stat.st_size / (1024 * 1024),
                'article_count': article_count,
                'modified_time': datetime.datetime.fromtimestamp(stat.st_mtime)
            }
        except Exception as e:
            logger.error(f"Error getting archive stats for {archive_path}: {e}")
            return None


# Singleton instance for application-wide use
_archive_writer_instance = None
_instance_lock = threading.Lock()


def get_archive_writer(base_archive_path: str = None) -> ArchiveWriter:
    """
    Get the singleton ArchiveWriter instance.

    :param base_archive_path: Base path for archives (required on first call)
    :return: ArchiveWriter instance
    """
    global _archive_writer_instance

    with _instance_lock:
        if _archive_writer_instance is None:
            if base_archive_path is None:
                raise ValueError("base_archive_path must be provided on first call")
            _archive_writer_instance = ArchiveWriter(base_archive_path)

        return _archive_writer_instance


# # end of file ##
