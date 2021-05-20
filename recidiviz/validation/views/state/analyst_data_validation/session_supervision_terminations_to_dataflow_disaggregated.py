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

"""A view which provides a person / day level comparison between supervision session ends and dataflow terminations"""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME = (
    "session_supervision_terminations_to_dataflow_disaggregated"
)

SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION = """
    A view which provides a person / day level comparison between supervision session ends and dataflow terminations. 
    For each person / day there are a set of binary variables that indicate whether that record meets a criteria. The 
    first two (session_end, session_with_end_reason) are used to identify sessions that do not have any dataflow end 
    reason associated with them. The second three (dataflow_termination, session_termination) are used to identify 
    dataflow terminations that re not represented in the sessions view. Note that a subset of termination reasons is 
    used in this comparison because of the fact that we would not expect every dataflow metric event to be associated
    with a compartment transition.
    """

SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH dataflow AS 
    (
    SELECT 
        * EXCEPT(end_date),
        DATE_SUB(end_date, INTERVAL 1 DAY) AS end_date,
        'SUPERVISION' AS compartment_level_0,
    FROM `{project_id}.{analyst_dataset}.compartment_session_end_reasons_materialized`
    WHERE end_reason NOT IN ('TRANSFER_WITHIN_STATE', 'INTERNAL_UNKNOWN', 'EXTERNAL_UNKNOWN')
        AND compartment_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE')
    )
    ,
    sessions AS
    (
    SELECT 
        *,
        'SUPERVISION' AS compartment_level_0,
    FROM `{project_id}.{analyst_dataset}.compartment_sessions_materialized` 
    WHERE compartment_level_1 IN ('SUPERVISION','SUPERVISION_OUT_OF_STATE')
    )
    SELECT
        person_id,
        state_code,
        end_date,
        sessions.session_id,
        dataflow.end_reason,
        CASE WHEN sessions.person_id IS NOT NULL THEN 1 ELSE 0 END as session_end,
        CASE WHEN dataflow.end_reason IS NOT NULL AND sessions.person_id IS NOT NULL THEN 1 ELSE 0 END AS session_with_end_reason,
        CASE WHEN dataflow.end_reason IS NOT NULL
            THEN 1 ELSE 0 END AS dataflow_termination,
        CASE WHEN dataflow.end_reason IS NOT NULL
            AND sessions.person_id IS NOT NULL THEN 1 ELSE 0 END AS session_termination,
    FROM sessions
    FULL OUTER JOIN dataflow
        USING(person_id, end_date, state_code, compartment_level_0)
    WHERE end_date IS NOT NULL
        AND EXTRACT(YEAR FROM end_date) > EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 20 YEAR))
    ORDER BY state_code, end_date
    """

SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME,
    view_query_template=SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE,
    description=SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSION_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED.build_and_print()
