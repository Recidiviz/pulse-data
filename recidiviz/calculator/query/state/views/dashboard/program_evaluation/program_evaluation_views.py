# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Dashboard views related to program evaluation."""
from typing import List

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.program_evaluation.us_nd.ftr_referrals_by_month import (
    FTR_REFERRALS_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.program_evaluation.us_nd.ftr_referrals_by_age_by_period import (
    FTR_REFERRALS_BY_AGE_BY_PERIOD_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.program_evaluation.us_nd.ftr_referrals_by_gender_by_period import (
    FTR_REFERRALS_BY_GENDER_BY_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.program_evaluation.us_nd.ftr_referrals_by_lsir_by_period import (
    FTR_REFERRALS_BY_LSIR_BY_PERIOD_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.program_evaluation.us_nd.ftr_referrals_by_participation_status import (
    FTR_REFERRALS_BY_PARTICIPATION_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.program_evaluation.us_nd.ftr_referrals_by_period import (
    FTR_REFERRALS_BY_PERIOD_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.program_evaluation.us_nd.ftr_referrals_by_race_and_ethnicity_by_period import (
    FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER,
)

PROGRAM_EVALUATION_VIEW_BUILDERS: List[MetricBigQueryViewBuilder] = [
    FTR_REFERRALS_BY_MONTH_VIEW_BUILDER,
    FTR_REFERRALS_BY_PERIOD_VIEW_BUILDER,
    FTR_REFERRALS_BY_AGE_BY_PERIOD_VIEW_BUILDER,
    FTR_REFERRALS_BY_GENDER_BY_PERIOD_VIEW_BUILDER,
    FTR_REFERRALS_BY_LSIR_BY_PERIOD_VIEW_BUILDER,
    FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER,
    FTR_REFERRALS_BY_PARTICIPATION_STATUS_VIEW_BUILDER,
]
