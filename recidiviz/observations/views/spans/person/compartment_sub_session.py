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
"""View with non-overlapping spans unique on all population attributes"""
from recidiviz.calculator.query.state.views.sessions.compartment_sub_sessions import (
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Non-overlapping spans unique on all population attributes"

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.COMPARTMENT_SUB_SESSION,
    description=_VIEW_DESCRIPTION,
    sql_source=COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER.table_for_query,
    attribute_cols=[
        "compartment_level_1",
        "compartment_level_2",
        "compartment_location",
        "facility",
        "facility_name",
        "supervision_office",
        "supervision_office_name",
        "supervision_district",
        "supervision_district_name",
        "supervision_region_name",
        "correctional_level",
        "correctional_level_raw_text",
        "housing_unit",
        "housing_unit_category",
        "housing_unit_type",
        "housing_unit_type_raw_text",
        "case_type",
        "prioritized_race_or_ethnicity",
        "gender",
        "age",
        "assessment_score",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
