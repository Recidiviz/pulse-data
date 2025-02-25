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
""""Views related to jail population."""
# pylint: disable=line-too-long

from recidiviz.calculator.query.county.views.population.population_admissions_releases_race_gender import \
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW
from recidiviz.calculator.query.county.views.population.population_admissions_releases_race_gender_all import \
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW
from recidiviz.calculator.query.county.views.population.population_admissions_releases import \
    POPULATION_ADMISSIONS_RELEASES_VIEW
from recidiviz.calculator.query.county.views.population.resident_population_counts import \
    RESIDENT_POPULATION_COUNTS_VIEW
from recidiviz.calculator.query.county.views.population.jail_pop_and_resident_pop import \
    JAIL_POP_AND_RESIDENT_POP_VIEW


POPULATION_VIEWS = [
    RESIDENT_POPULATION_COUNTS_VIEW,
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW,
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_ALL_VIEW,
    POPULATION_ADMISSIONS_RELEASES_VIEW,
    JAIL_POP_AND_RESIDENT_POP_VIEW
]
