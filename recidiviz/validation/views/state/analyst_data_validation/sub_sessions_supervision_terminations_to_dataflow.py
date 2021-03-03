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

"""A view which provides an annual comparison between supervision session ends and dataflow terminations."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUB_SESSIONS_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_NAME = (
    "sub_sessions_supervision_terminations_to_dataflow"
)

SUB_SESSIONS_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DESCRIPTION = """
    A view which provides an annual comparison between supervision session ends and dataflow terminations. One
    comparison is session ends vs sessions_with_end_reason (the latter being a subset of the former), which can be used 
    to identify the % of sessions with end reasons. Another comparison is dataflow_terminations vs 
    session_terminations (the latter being a subset of the former), which can be used to identify the % of dataflow 
    terminations events represented in sessions
    """

SUB_SESSIONS_SUPERVISION_TERMINATIONS_TO_DATAFLOW_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        state_code AS region_code,
        DATE_TRUNC(end_date, YEAR) AS end_year,
        SUM(sub_session_end) AS sub_session_ends,
        SUM(session_end) AS session_ends,
        SUM(sub_session_with_end_reason) AS sub_sessions_with_end_reason,
        SUM(session_with_end_reason) AS sessions_with_end_reason,
        SUM(dataflow_termination) AS dataflow_terminations,
        SUM(sub_session_termination) AS sub_session_terminations,
        SUM(session_termination) AS session_terminations
    FROM `{project_id}.{validation_views_dataset}.sub_sessions_supervision_terminations_to_dataflow_disaggregated`
    GROUP BY 1,2
    ORDER BY 1,2
    """

SUB_SESSIONS_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUB_SESSIONS_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_NAME,
    view_query_template=SUB_SESSIONS_SUPERVISION_TERMINATIONS_TO_DATAFLOW_QUERY_TEMPLATE,
    description=SUB_SESSIONS_SUPERVISION_TERMINATIONS_TO_DATAFLOW_DESCRIPTION,
    validation_views_dataset=dataset_config.VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUB_SESSIONS_SUPERVISION_TERMINATIONS_TO_DATAFLOW_VIEW_BUILDER.build_and_print()
