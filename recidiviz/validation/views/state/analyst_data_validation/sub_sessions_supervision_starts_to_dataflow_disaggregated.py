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

"""A view which provides a person / day level comparison between supervision session starts and dataflow supervision
starts"""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUB_SESSIONS_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME = (
    "sub_sessions_supervision_starts_to_dataflow_disaggregated"
)

SUB_SESSIONS_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION = """
    A view which provides a person / day level comparison between supervision session starts and dataflow supervision
    starts. For each person / day there are a set of binary variables that indicate whether that record meets a 
    criteria. The first four (sub_session_start, session_start, sub_session_with_start_reason, 
    session_with_start_reason) are used to identify sessions or sub sessions that do not have any dataflow start reason 
    associated with them. The second three (dataflow_supervision_start, sub_session_supervision_start, 
    session_supervision_start) are used to identify dataflow admissions that are not represented in the sessions or 
    sub-sessions views. Note that a subset of admission reasons is used in this comparison because of the fact that we 
    would not expect every dataflow metric event to be associated with a compartment transition
    """

SUB_SESSIONS_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        person_id,
        state_code,
        start_date,
        compartment_level_1,
        sessions.session_id,
        sessions.sub_session_id,
        dataflow.start_reason,
        CASE WHEN sessions.person_id IS NOT NULL THEN 1 ELSE 0 END as sub_session_start,
        COALESCE(sessions.first_sub_session_in_session, 0) AS session_start,
        CASE when dataflow.start_reason IS NOT NULL AND sessions.person_id IS NOT NULL THEN 1 ELSE 0 END AS sub_session_with_start_reason,
        CASE when dataflow.start_reason IS NOT NULL AND first_sub_session_in_session = 1 THEN 1 ELSE 0 END AS session_with_start_reason,
        CASE WHEN dataflow.start_reason IN ('CONDITIONAL_RELEASE','COURT_SENTENCE')
            THEN 1 ELSE 0 END AS dataflow_supervision_start,
        CASE WHEN dataflow.start_reason IN ('CONDITIONAL_RELEASE','COURT_SENTENCE') 
            AND sessions.person_id IS NOT NULL 
            THEN 1 ELSE 0 END AS sub_session_supervision_start,
        CASE WHEN dataflow.start_reason IN ('CONDITIONAL_RELEASE','COURT_SENTENCE') 
            AND sessions.first_sub_session_in_session = 1 
            THEN 1 ELSE 0 END AS session_supervision_start,
    FROM `{project_id}.{analyst_dataset}.compartment_sub_sessions_materialized` sessions
    FULL OUTER JOIN `{project_id}.{analyst_dataset}.compartment_session_start_reasons_materialized` dataflow
        USING(person_id, start_date, state_code, compartment_level_1)
    WHERE compartment_level_1 ='SUPERVISION'
        AND EXTRACT(YEAR FROM start_date) > EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 20 YEAR))
    ORDER BY state_code, start_date
    """

SUB_SESSIONS_SUPERVISION_STARTS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUB_SESSIONS_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_VIEW_NAME,
    view_query_template=SUB_SESSIONS_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_QUERY_TEMPLATE,
    description=SUB_SESSIONS_SUPERVISION_STARTS_TO_DATAFLOW_DISAGGREGATED_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUB_SESSIONS_SUPERVISION_STARTS_TO_DATAFLOW_VIEW_BUILDER_DISAGGREGATED.build_and_print()
