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
"""Views related to supervision."""
# pylint: disable=line-too-long
from recidiviz.calculator.query.state.views.supervision.us_nd.average_change_lsir_score_by_month import \
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW
from recidiviz.calculator.query.state.views.supervision.supervision_population_by_age_60_days import \
    SUPERVISION_POPULATION_BY_AGE_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.supervision.supervision_population_by_gender_60_days import \
    SUPERVISION_POPULATION_BY_GENDER_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.supervision.supervision_population_by_lsir_60_days import \
    SUPERVISION_POPULATION_BY_LSIR_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.supervision.supervision_population_by_race_and_ethnicity_60_days import \
    SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.supervision.supervision_termination_by_type_by_month import \
    SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW

SUPERVISION_VIEWS = [
    AVERAGE_CHANGE_LSIR_SCORE_MONTH_VIEW,
    SUPERVISION_POPULATION_BY_AGE_60_DAYS_VIEW,
    SUPERVISION_POPULATION_BY_GENDER_60_DAYS_VIEW,
    SUPERVISION_POPULATION_BY_LSIR_60_DAYS_VIEW,
    SUPERVISION_POPULATION_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW,
    SUPERVISION_TERMINATION_BY_TYPE_BY_MONTH_VIEW
]
