# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# ============================================================================
"""Wrappers around uszipcode to handle cleanup properly.

uszipcode.SearchEngine keeps a single SQLAlchemy session open for its entire lifetime and uses it to query zipcode
information from its local SQLite database. It relies on __init__ to open the session and __del__ to close it. Using
__del__ in that way is problematic because we want to keep a single, global SearchEngine around, and the interpreter
does not guarantee when __del__ will be called. It is frequently called after other modules (like SQLAlchemy itself or
one of its dependencies) have been deleted and causes our program to crash during termination.

Instead, we modify SearchEngine to not manage its SQLAlchemy session lifetime and instead manage it ourself using a
contextmanager.
"""

from contextlib import contextmanager
import logging
from typing import Tuple

import requests
from sqlalchemy.orm.session import sessionmaker
from uszipcode import SearchEngine
import uszipcode.db
import uszipcode.search

# uszipcode can accidentally skip chunks when downloading files, reimplement it.
def download_simple_db_file(db_file_dir):
    logging.info('Download simple zipcode file safely')
    simple_db_file_download_url = "https://datahub.io/machu-gwu/uszipcode-0.2.0-simple_db/r/simple_db.sqlite"

    if not uszipcode.db.is_simple_db_file_exists(db_file_dir):
        logging.info("Start downloading data for simple zipcode database, total size 9MB ...")
        with requests.get(simple_db_file_download_url, stream=True) as r:
            r.raise_for_status()

            chunk_size = 1 * 1024 ** 2  # 1 MB
            downloaded = 0
            with uszipcode.db.atomic_write(uszipcode.db.get_simple_db_file_path(db_file_dir).abspath,
                                           mode="wb", overwrite=True) as f:
                for chunk in r.iter_content(chunk_size):
                    f.write(chunk)
                    downloaded += len(chunk)
                    logging.info("%.1f MB finished ...", downloaded / chunk_size)
        logging.info("Complete!")

# Override the prior implementation
uszipcode.search.download_simple_db_file = download_simple_db_file

_ZIP_CODE_SEARCH_ENGINE: SearchEngine = None
_SESSION_MAKER: sessionmaker = None
def _get_zipcode_globals() -> Tuple[SearchEngine, sessionmaker]:
    global _ZIP_CODE_SEARCH_ENGINE, _SESSION_MAKER

    if _ZIP_CODE_SEARCH_ENGINE is None or _SESSION_MAKER is None:
        _ZIP_CODE_SEARCH_ENGINE = SearchEngine(simple_zipcode=True)
        # Close the initial session that it creates, we will make our own later.
        _ZIP_CODE_SEARCH_ENGINE.close()
        _ZIP_CODE_SEARCH_ENGINE.ses = None
        # Create a sessionmaker to use for creating our own sessions since SearchEngine threw its away.
        _SESSION_MAKER = sessionmaker(bind=_ZIP_CODE_SEARCH_ENGINE.engine)

    return _ZIP_CODE_SEARCH_ENGINE, _SESSION_MAKER


@contextmanager
def zipcode_search_engine() -> SearchEngine:
    """Provides access to SearchEngine.

    Handles opening and closing a new session to be used within the context.
    """
    engine, session_maker = _get_zipcode_globals()
    engine.ses = session_maker()
    try:
        yield engine
    finally:
        engine.close()
        engine.ses = None
