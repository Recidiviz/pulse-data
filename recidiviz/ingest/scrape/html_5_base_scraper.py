# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

import html5lib
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.task_params import ScrapedData, Task


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
    def populate_data(
        self, content, task: Task, ingest_info: IngestInfo
    ) -> ScrapedData:
        pass
