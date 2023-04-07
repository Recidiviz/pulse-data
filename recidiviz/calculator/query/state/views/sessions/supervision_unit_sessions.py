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
"""Sessionized view of each individual on supervision. Session defined as continuous
time on caseload associated with a given supervision unit, based on the relationship
between officers and their supervisor unit.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    nonnull_end_date_clause,
    nonnull_end_date_exclusive_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_UNIT_SESSIONS_VIEW_NAME = "supervision_unit_sessions"

SUPERVISION_UNIT_SESSIONS_VIEW_DESCRIPTION = """
Sessionized view of each individual. Session defined as continuous stay on supervision 
associated with a given supervision unit. Unit sessions may be overlapping.
"""

SUPERVISION_UNIT_SESSIONS_QUERY_TEMPLATE = f"""
WITH overlapping_spans AS (
    SELECT
        a.state_code,
        a.person_id,
        GREATEST(a.start_date, b.start_date) AS start_date,
        {revert_nonnull_end_date_clause(f"LEAST({nonnull_end_date_clause('a.end_date_exclusive')}, "
                                        f"{nonnull_end_date_clause('b.end_date_exclusive')})")} AS end_date,
        supervision_district,
        supervision_unit,
        supervision_unit_name,
    FROM
        `{{project_id}}.{{sessions_dataset}}.supervision_officer_sessions_materialized` a
    INNER JOIN
        `{{project_id}}.{{sessions_dataset}}.supervision_officer_attribute_sessions_materialized` b
    ON
        a.state_code = b.state_code
        AND a.supervising_officer_external_id = b.officer_id
        AND (
            a.start_date BETWEEN b.start_date AND {nonnull_end_date_exclusive_clause("b.end_date_exclusive")}
            OR b.start_date BETWEEN a.start_date AND {nonnull_end_date_exclusive_clause("a.end_date_exclusive")}
        )
)
,
{create_sub_sessions_with_attributes(table_name="overlapping_spans")},
sub_sessions_dedup_cte AS (
    SELECT DISTINCT
        state_code,
        person_id,
        start_date,
        end_date,
        supervision_district,
        supervision_unit,
        supervision_unit_name,
    FROM
        sub_sessions_with_attributes
)
SELECT
    *,
    end_date AS end_date_exclusive,
FROM ({aggregate_adjacent_spans(
    table_name="sub_sessions_dedup_cte",
    attribute=["supervision_district", "supervision_unit", "supervision_unit_name"],
    session_id_output_name="supervision_unit_session_id",
    end_date_field_name="end_date"
)})

"""

SUPERVISION_UNIT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_UNIT_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_UNIT_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_UNIT_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_UNIT_SESSIONS_VIEW_BUILDER.build_and_print()
