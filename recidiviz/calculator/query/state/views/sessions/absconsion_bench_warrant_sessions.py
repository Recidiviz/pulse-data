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
"""Sessionized view of continuous periods of stay on absconsion/bench warrant status"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_NAME = "absconsion_bench_warrant_sessions"

ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_DESCRIPTION = """Sessionized view of continuous periods of stay on absconsion/bench warrant status"""

ABSCONSION_BENCH_WARRANT_SESSIONS_QUERY_TEMPLATE = f"""
    WITH absconsion_periods AS (
            -- via compartment change
        SELECT
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            inflow_from_level_1,
            inflow_from_level_2,
        FROM
            `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` a
        WHERE
            compartment_level_1 = "SUPERVISION"
            AND compartment_level_2 IN ("ABSCONSION", "BENCH_WARRANT")
    )
    SELECT 
        *,
        end_date_exclusive AS end_date,
    FROM (
        {aggregate_adjacent_spans(
            table_name="absconsion_periods",
            attribute=["inflow_from_level_1","inflow_from_level_2"],
            session_id_output_name='absconsion_bench_warrant_session_id',
            end_date_field_name='end_date_exclusive'
        )}
    )
"""

ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_NAME,
    view_query_template=ABSCONSION_BENCH_WARRANT_SESSIONS_QUERY_TEMPLATE,
    description=ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER.build_and_print()
