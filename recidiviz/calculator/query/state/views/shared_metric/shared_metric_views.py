# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Reference views used by other views that rely on Dataflow metrics."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.shared_metric.admission_types_per_state_for_matrix import (
    ADMISSION_TYPES_PER_STATE_FOR_MATRIX_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.event_based_admissions import (
    EVENT_BASED_ADMISSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.event_based_commitments_from_supervision import (
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.event_based_commitments_from_supervision_for_matrix import (
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.event_based_program_referrals import (
    EVENT_BASED_PROGRAM_REFERRALS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.event_based_supervision_populations import (
    EVENT_BASED_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.event_based_supervision_populations_with_commitments_for_rate_denominators import (
    EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.overdue_discharge_alert_exclusions import (
    OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.overdue_discharge_outcomes import (
    OVERDUE_DISCHARGE_OUTCOMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.revocations_matrix_by_person import (
    REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.single_day_incarceration_population_for_spotlight import (
    SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.single_day_supervision_population_for_spotlight import (
    SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_case_compliance_metrics import (
    SUPERVISION_CASE_COMPLIANCE_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_matrix_by_person import (
    SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_mismatches_by_day import (
    SUPERVISION_MISMATCHES_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_officer_caseload import (
    SUPERVISION_OFFICER_CASELOAD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_termination_matrix_by_person import (
    SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER,
)

SHARED_METRIC_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_BUILDER,
    EVENT_BASED_ADMISSIONS_VIEW_BUILDER,
    EVENT_BASED_PROGRAM_REFERRALS_VIEW_BUILDER,
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER,
    EVENT_BASED_SUPERVISION_VIEW_BUILDER,
    EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_VIEW_BUILDER,
    REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER,
    SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER,
    ADMISSION_TYPES_PER_STATE_FOR_MATRIX_VIEW_BUILDER,
    SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER,
    SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
    SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
    OVERDUE_DISCHARGE_OUTCOMES_VIEW_BUILDER,
    OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_VIEW_BUILDER,
    SUPERVISION_CASE_COMPLIANCE_METRICS_VIEW_BUILDER,
    SUPERVISION_MISMATCHES_BY_DAY_VIEW_BUILDER,
    SUPERVISION_OFFICER_CASELOAD_VIEW_BUILDER,
]
