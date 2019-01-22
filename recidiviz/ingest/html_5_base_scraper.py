# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
# =============================================================================

"""Abstract class based on BaseScraper. Works exactly like BaseScraper and
must be subclassed in the same way, except that this scraper uses html5lib to
preprocess HTML content that may only be accepted in the HTML5 spec.
"""

import abc
import xml.etree.ElementTree
from typing import Optional

import html5lib
from lxml import html

from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.models.ingest_info import IngestInfo


class Html5BaseScraper(BaseScraper):
    """Generic class for scrapers that parse HTML5 content."""

    def _parse_html_content(self, content_string: str) -> html.HtmlElement:
        # html5lib supports tags that are not closed, so parse twice to
        # convert from html5lib's xml.etree.ElementTree to lxml's
        # lxml.html.HtmlElement.
        html5_etree = html5lib.parse(content_string)
        html5_string = xml.etree.ElementTree.tostring(html5_etree)
        return html.fromstring(html5_string)

    @abc.abstractmethod
    def get_more_tasks(self, content, params):
        pass

    @abc.abstractmethod
    def populate_data(self, content, params,
                      ingest_info: IngestInfo) -> Optional[IngestInfo]:
        pass
