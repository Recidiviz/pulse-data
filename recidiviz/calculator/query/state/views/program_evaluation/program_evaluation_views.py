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
"""Views related to program evaluation."""
# pylint: disable=line-too-long, trailing-whitespace
from recidiviz.calculator.query.state.views.program_evaluation.us_nd.ftr_referrals_by_month import \
    FTR_REFERRALS_BY_MONTH_VIEW
from recidiviz.calculator.query.state.views.program_evaluation.us_nd.ftr_referrals_by_age_60_days import \
    FTR_REFERRALS_BY_AGE_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.program_evaluation.us_nd.ftr_referrals_by_gender_60_days import \
    FTR_REFERRALS_BY_GENDER_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.program_evaluation.us_nd.ftr_referrals_by_lsir_60_days import \
    FTR_REFERRALS_BY_LSIR_60_DAYS_VIEW
from recidiviz.calculator.query.state.views.program_evaluation.us_nd.ftr_referrals_by_race_and_ethnicity_60_days import \
    FTR_REFERRALS_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW

PROGRAM_EVALUATION_VIEWS = [
    FTR_REFERRALS_BY_MONTH_VIEW,
    FTR_REFERRALS_BY_AGE_60_DAYS_VIEW,
    FTR_REFERRALS_BY_GENDER_60_DAYS_VIEW,
    FTR_REFERRALS_BY_LSIR_60_DAYS_VIEW,
    FTR_REFERRALS_BY_RACE_AND_ETHNICITY_60_DAYS_VIEW,
]
