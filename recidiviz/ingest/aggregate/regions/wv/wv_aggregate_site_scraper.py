# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Returns the csv urls for West Virginia state jail data collection."""
import ssl
from typing import Any, Set

import requests
from lxml import html
from requests.adapters import HTTPAdapter
from urllib3 import poolmanager

STATE_AGGREGATE_URL = (
    "https://dhhr.wv.gov/COVID-19/Pages/Correctional-Facilities-report-archive.aspx"
)


class TLSAdapter(HTTPAdapter):
    """Lower the security level for dhhr.wv.gov"""

    def init_poolmanager(
        self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any
    ) -> None:
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_version=ssl.PROTOCOL_TLS,
            ssl_context=ctx,
            **pool_kwargs,
        )


def get_urls_to_download() -> Set[str]:
    session = requests.session()
    session.mount("https://dhhr.wv.gov", TLSAdapter())
    response = session.get(STATE_AGGREGATE_URL)
    content = html.fromstring(response.text)

    return {
        f"https://dhhr.wv.gov{url}"
        for url in content.xpath("//a/@href")
        if url.startswith("/COVID-19/Documents/COVID19_DCR_")
        and url.endswith(".txt")
        and not url.endswith("COVID19_DCR_2020_09-25.txt")
    }
