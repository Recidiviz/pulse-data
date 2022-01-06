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

"""Scrapes the California aggregate site and finds pdfs to download."""
import datetime
import re
from typing import Any, Dict, List, Tuple

import requests

from recidiviz.common import str_field_utils
from recidiviz.ingest.aggregate import aggregate_ingest_utils

# Human landing page: https://app.bscc.ca.gov/joq//jps/QuerySelection.asp
LANDING_PAGE = "https://app.bscc.ca.gov/joq//jps/query.asp?action=v"
DATE_RANGE_RE = r"(.*) through (.*)"
PDF_URL = "https://app.bscc.ca.gov/joq//jps/query.asp?action=q"
DATE_RANGE_ANCHOR = "Data is available from"


def _get_landing_data() -> Dict[str, str]:
    return {"DataType": "Facility", "ReportingRange": "2002", "Continue": "Continue"}


def _get_pdf_data(
    year: int, month_from: int, month_to: int, reporting_range: int
) -> Dict[str, Any]:
    # This is the post data we need to return a pdf with all counties in
    # California set.  The IDs are presumably ids used internally on the site
    # and they were acquired by checking the post data on the California
    # aggregate report site.
    return {
        "DataType": "Facility",
        "ReportingRange": reporting_range,
        "year": year,
        "Month_From": month_from,
        "Month_To": month_to,
        "Sort_By": "Date",
        "jurisdictions": [
            "6",
            "21",
            "25",
            "28",
            "500",
            "36",
            "44",
            "50",
            "505",
            "57",
            "61",
            "64",
            "68",
            "76",
            "84",
            "89",
            "93",
            "97",
            "101",
            "108",
            "125",
            "132",
            "134",
            "140",
            "145",
            "150",
            "156",
            "161",
            "168",
            "177",
            "12",
            "181",
            "185",
            "190",
            "196",
            "204",
            "209",
            "216",
            "220",
            "221",
            "222",
            "236",
            "241",
            "244",
            "252",
            "614",
            "257",
            "264",
            "260",
            "272",
            "109",
            "550",
            "277",
            "280",
            "285",
            "288",
            "292",
            "297",
            "304",
            "575",
            "309",
            "314",
            "316",
            "321",
            "324",
            "325",
            "332",
            "340",
        ],
        "variables": ["2", "3", "4", "5", "6"],
        "RunQuery": "Excel",
    }


def get_urls_to_download() -> List[Tuple[str, Dict]]:
    """Get all of the urls that should be downloaded."""
    page = requests.post(LANDING_PAGE, data=_get_landing_data()).text

    # Formatting on the page is extremely weird so its easiest to just take a
    # slice of the data.
    start = page.index(DATE_RANGE_ANCHOR) + len(DATE_RANGE_ANCHOR) + 10
    end = start + 50
    match = re.match(DATE_RANGE_RE, page[start:end])
    if match:
        date_from = str_field_utils.parse_date(match.group(1))
        date_to = str_field_utils.parse_date(match.group(2))

    if not (match and date_from and date_to):
        date_from = datetime.date(year=1995, month=9, day=5)
        date_to = aggregate_ingest_utils.subtract_month(
            datetime.date.today().replace(day=1)
        )

    aggregate_urls = []
    for i in range(date_from.year, date_to.year + 1):
        month_from = 1
        month_to = 12
        if i == date_from.year:
            month_from = date_from.month
        elif i == date_to.year:
            month_to = date_to.month
        reporting_range = 1995 if i < 2002 else 2002
        pdf_post_data = _get_pdf_data(i, month_from, month_to, reporting_range)
        aggregate_urls.append((PDF_URL, pdf_post_data))
    return aggregate_urls
