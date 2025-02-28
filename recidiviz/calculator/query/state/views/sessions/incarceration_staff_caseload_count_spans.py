# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
View that sessionizes periods of time over which an incarceration staff is assigned
a caseload of some size.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.sessions.staff_caseload_count_spans import (
    build_caseload_count_spans_query_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "incarceration_staff_caseload_count_spans"

_VIEW_DESCRIPTION = """
View that sessionizes periods of time over which an incarceration staff is assigned
a caseload of some size.
"""

_QUERY_TEMPLATE = build_caseload_count_spans_query_template(
    assignment_sessions_sql_source="""
    SELECT * EXCEPT(incarceration_staff_assignment_id), incarceration_staff_assignment_id AS incarceration_staff_id,
    FROM `{project_id}.sessions.incarceration_staff_assignment_sessions_preprocessed_materialized`
""",
    index_cols=["state_code", "incarceration_staff_id"],
    start_date_col_name="start_date",
    end_date_exclusive_col_name="end_date_exclusive",
)

INCARCERATION_STAFF_CASELOAD_COUNT_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    clustering_fields=["state_code", "incarceration_staff_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_STAFF_CASELOAD_COUNT_SPANS_VIEW_BUILDER.build_and_print()
