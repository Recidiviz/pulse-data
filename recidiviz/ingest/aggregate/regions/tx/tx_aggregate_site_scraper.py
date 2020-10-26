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

"""Scrapes the texas aggregate site and finds pdfs to download."""
from typing import Set

import requests
from lxml import html

STATE_AGGREGATE_URL = 'https://www.tcjs.state.tx.us/historical-population-reports/'


def get_urls_to_download() -> Set[str]:
    """Scrapes the aggregate report pdfs from the state website"""
    page = requests.get(STATE_AGGREGATE_URL).text
    html_tree = html.fromstring(page)
    links = html_tree.xpath('//a/@href')
    return {link for link in links if 'Abbre' in link and '.pdf' in link}
