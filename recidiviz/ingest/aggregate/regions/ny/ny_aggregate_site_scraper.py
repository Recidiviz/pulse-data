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

"""Scrapes the new york aggregate site and finds pdfs to download."""
from typing import Set

STATE_AGGREGATE_URL = (
    "https://www.criminaljustice.ny.gov/crimnet/ojsa/jail_population.pdf"
)


def get_urls_to_download() -> Set[str]:
    # NY just has one years worth of data in one table.
    return {STATE_AGGREGATE_URL}
