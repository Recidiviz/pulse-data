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
"""Dashboard views related to revocation analysis."""

from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_supervision_location_ids_to_names import (
    REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.state_gender_population import (
    STATE_GENDER_POPULATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.state_race_ethnicity_population import (
    STATE_RACE_ETHNICITY_POPULATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_location_ids_to_names import (
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_by_month import (
    REVOCATIONS_MATRIX_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_cells import (
    REVOCATIONS_MATRIX_CELLS_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_distribution_by_district import (
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_distribution_by_gender import (
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_distribution_by_race import (
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_distribution_by_risk_level import (
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_distribution_by_violation import (
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_filtered_caseload import (
    REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER,
)


from recidiviz.calculator.query.state.views.dashboard.revocation_analysis.revocations_matrix_distribution_by_officer import (
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_location_restricted_access_emails import (
    SUPERVISION_LOCATION_RESTRICTED_ACCESS_EMAILS_VIEW_BUILDER,
)

REVOCATION_ANALYSIS_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    REVOCATIONS_MATRIX_BY_MONTH_VIEW_BUILDER,
    REVOCATIONS_MATRIX_CELLS_VIEW_BUILDER,
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_DISTRICT_VIEW_BUILDER,
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_GENDER_VIEW_BUILDER,
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_RACE_VIEW_BUILDER,
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_RISK_LEVEL_VIEW_BUILDER,
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_VIOLATION_VIEW_BUILDER,
    REVOCATIONS_MATRIX_FILTERED_CASELOAD_VIEW_BUILDER,
    REVOCATIONS_MATRIX_DISTRIBUTION_BY_OFFICER_VIEW_BUILDER,
    REVOCATIONS_MATRIX_SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
    SUPERVISION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
    SUPERVISION_LOCATION_RESTRICTED_ACCESS_EMAILS_VIEW_BUILDER,
    STATE_RACE_ETHNICITY_POPULATION_VIEW_BUILDER,
    STATE_GENDER_POPULATION_VIEW_BUILDER,
]
