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

"""Object representing a successful scrape session. This is stored
only when a scraper has been determined to have completed and written
everything it will to the database.
"""
import datetime
import attr

from recidiviz.ingest.models.model_utils import date_converter_or_today


@attr.s(frozen=True)
class ScraperSuccess:
    """Scraper success model"""

    # Date, or today
    date: datetime.date = attr.ib(default=None, converter=date_converter_or_today)
