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
"""Views interacting with Vera data."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.county.views.vera import (
    county_names,
    region_fips_map,
    scraper_success_rate,
    total_resident_population_counts,
    urbanicity,
)

VERA_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    county_names.COUNTY_NAMES_VIEW_BUILDER,
    region_fips_map.REGION_FIPS_MAP_VIEW_BUILDER,
    scraper_success_rate.SCRAPER_SUCCESS_RATE_VIEW_BUILDER,
    total_resident_population_counts.TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_BUILDER,
    urbanicity.URBANICITY_VIEW_BUILDER,
]
