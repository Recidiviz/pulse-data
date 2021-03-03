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

"""Tools for identifying scrapers."""


class ScrapeKey:
    """A key defining a conceptual scraper, i.e. region plus scrape type."""

    def __init__(self, region_code, scrape_type):
        if not region_code or not scrape_type:
            raise ValueError(
                "A scrape key must include both a region code " "and a scrape type"
            )
        self.region_code = region_code
        self.scrape_type = scrape_type

    def __repr__(self):
        return "<ScrapeKey region_code: {}, scrape_type: {}>".format(
            self.region_code, self.scrape_type
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (
                self.region_code == other.region_code
                and self.scrape_type == other.scrape_type
            )
        return False
