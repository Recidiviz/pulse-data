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

"""Scrapes the georgia aggregate site and finds pdfs to download."""
import re
from typing import Set

import requests
from lxml import html

from recidiviz.utils.string import StrictStringFormatter

STATE_AGGREGATE_URL = "https://www.dca.ga.gov/node/3811/documents/2086"
BASE_URL = "https://www.dca.ga.gov{path}"
YEAR_PATTERN = re.compile(r"[0-9]{4} Jail Reports")


def get_urls_to_download() -> Set[str]:
    page = requests.get(STATE_AGGREGATE_URL).text
    html_tree = html.fromstring(page)
    links = html_tree.xpath("//a")

    aggregate_report_urls = set()
    for link in links:
        if YEAR_PATTERN.match(link.text_content()):
            url = StrictStringFormatter().format(BASE_URL, path=link.attrib["href"])
            # We need to do a separate get request on the actual report page
            page = requests.get(url).text
            html_tree = html.fromstring(page)
            links_year = html_tree.xpath("//a/@href")
            for link_year in links_year:
                if "jail_report" in link_year and ".pdf" in link_year:
                    aggregate_report_urls.add(link_year)
    return aggregate_report_urls
