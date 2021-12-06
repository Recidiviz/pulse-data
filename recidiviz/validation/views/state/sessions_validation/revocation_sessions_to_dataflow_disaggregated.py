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

"""A view which provides a person / day level comparison between session revocations and dataflow revocations"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME = (
    "revocation_sessions_to_dataflow_disaggregated"
)

REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION = """
A view which provides a person / day level comparison between session identified revocations and the 
revocation admissions in the incarceration commitment from supervision dataflow 
metric. For each person / revocation date there are a set of binary variables that 
indicate whether the revocation appears in dataflow, sessions, or both.
"""

REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH dataflow_revocations AS
    (
    SELECT DISTINCT
        person_id,
        state_code,
        admission_date AS revocation_date
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized`
    WHERE admission_reason = 'REVOCATION' 
    )
    ,
    session_revocations AS
    (
    SELECT
        person_id,
        state_code,
        revocation_date
    FROM `{project_id}.{sessions_dataset}.revocation_sessions_materialized` 
    WHERE revocation_date IS NOT NULL
    )
    SELECT 
        person_id,
        state_code,
        revocation_date,
        CASE WHEN s.revocation_date IS NOT NULL THEN 1 ELSE 0 END AS in_sessions,
        CASE WHEN d.revocation_date IS NOT NULL THEN 1 ELSE 0 END AS in_dataflow,
        CASE WHEN s.revocation_date IS NOT NULL AND d.revocation_date IS NOT NULL THEN 1 ELSE 0 END AS in_both,
    FROM dataflow_revocations d
    FULL OUTER JOIN session_revocations s
        USING(person_id, state_code, revocation_date)
    WHERE EXTRACT(YEAR FROM revocation_date) > EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 20 YEAR))
    ORDER BY 1,2,3
    """

REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME,
    view_query_template=REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE,
    description=REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_SESSIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_BUILDER.build_and_print()
