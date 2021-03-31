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

"""A view which provides an annual comparison between reincarceration sessions and dataflow recidivism counts."""

# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_VIEW_NAME = (
    "reincarcerations_from_sessions_to_dataflow"
)

REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_DESCRIPTION = """
    A view which provides an annual comparison between reincarceration sessions and dataflow recidivism counts. The 
    view shows the annual counts of each individually and the counts of person/days in both. The view also shows the 
    count of events where the days between release and reincarceration match exactly. 
    """

REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        state_code AS region_code,
        DATE_TRUNC(reincarceration_date, YEAR) AS reincarceration_date,
        SUM(in_sessions) AS in_sessions,
        SUM(in_dataflow) AS in_dataflow,
        SUM(in_both) AS in_both,
        SUM(release_days_match) AS release_days_match
    FROM `{project_id}.{validation_views_dataset}.reincarcerations_from_sessions_to_dataflow_disaggregated`
    GROUP BY 1,2
    ORDER BY 1,2
    """

REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_VIEW_NAME,
    view_query_template=REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_QUERY_TEMPLATE,
    description=REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_DESCRIPTION,
    validation_views_dataset=dataset_config.VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REINCARCERATIONS_FROM_SESSIONS_TO_DATAFLOW_VIEW_BUILDER.build_and_print()
