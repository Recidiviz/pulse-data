# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View with spans of time during which an individual has certain compliance requirements + statuses."""
from recidiviz.calculator.query.state.views.vitals_report.supervision_case_compliance_spans import (
    SUPERVISION_CASE_COMPLIANCE_SPANS_VIEW_BUILDER,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Spans of time during which an individual has certain compliance requirements + statuses"

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.SUPERVISION_CASE_COMPLIANCE_SPAN,
    description=_VIEW_DESCRIPTION,
    sql_source=SUPERVISION_CASE_COMPLIANCE_SPANS_VIEW_BUILDER.table_for_query,
    attribute_cols=[
        "assessment_required",
        "assessment_overdue",
        "contact_required",
        "contact_overdue",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
