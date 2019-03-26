# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

from recidiviz.calculator.bq.views.population.population_admissions_releases_race_gender import \
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW
from recidiviz.calculator.bq.views.population.population_admissions_releases import \
    POPULATION_ADMISSIONS_RELEASES_VIEW


POPULATION_VIEWS = [
    POPULATION_ADMISSIONS_RELEASES_RACE_GENDER_VIEW,
    POPULATION_ADMISSIONS_RELEASES_VIEW
]
