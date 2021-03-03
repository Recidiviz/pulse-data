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

"""Scrapes the hawaii aggregate site and finds pdfs to download."""
import datetime
from typing import Set
from lxml import html
import requests

from recidiviz.ingest.aggregate.regions.hi.hi_aggregate_ingest import parse_date

STATE_AGGREGATE_URL = "http://dps.hawaii.gov/about/divisions/corrections/"
ACCEPTABLE_DATE = datetime.date(year=2017, month=8, day=31)


def get_urls_to_download() -> Set[str]:
    page = requests.get(STATE_AGGREGATE_URL).text
    html_tree = html.fromstring(page)
    links = html_tree.xpath("//a/@href")

    aggregate_report_urls = set()
    for link in links:
        if "Pop-Reports-EOM" in link and link.endswith(".pdf"):
            d = parse_date(link)
            if d >= ACCEPTABLE_DATE:
                aggregate_report_urls.add(link)
    return aggregate_report_urls
