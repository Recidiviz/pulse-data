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

"""Scrapes the MA aggregate site and finds pdfs to download.

Author: Albert Sun, 7/22/2021"""
from typing import Set

import requests
from lxml import html

STATE_AGGREGATE_URL = "https://www.mass.gov/report/research-reports"
BASE_URL = "https://www.mass.gov{}"


def get_urls_to_download() -> Set[str]:
    page = requests.get(STATE_AGGREGATE_URL).text
    html_tree = html.fromstring(page)
    year_links = html_tree.xpath("//a")

    aggregate_report_urls = set()

    for year_link in year_links:
        year_url = BASE_URL.format(year_link.attrib["href"])
        if "lists/weekly-inmate-count-" in year_url:
            page = requests.get(year_url).text
            html_tree = html.fromstring(page)
            week_urls = html_tree.xpath("//a/@href")
            for week_url in week_urls:
                if "download" in week_url:
                    aggregate_report_urls.add(week_url)
    return aggregate_report_urls
