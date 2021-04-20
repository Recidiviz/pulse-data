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

REVOCATION_SESSIONS_TO_DATAFLOW_VIEW_NAME = "revocation_sessions_to_dataflow"

REVOCATION_SESSIONS_TO_DATAFLOW_DESCRIPTION = """
    A view which provides an annual comparison between revocation sessions and dataflow revocation counts. The 
    view shows the annual counts of each individually and the counts of person/days in both.
    """

REVOCATION_SESSIONS_TO_DATAFLOW_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        state_code AS region_code,
        DATE_TRUNC(revocation_date, YEAR) AS revocation_date,
        SUM(in_sessions) AS in_sessions,
        SUM(in_dataflow) AS in_dataflow,
        SUM(in_both) AS in_both,
    FROM `{project_id}.{validation_views_dataset}.revocation_sessions_to_dataflow_disaggregated`
    GROUP BY 1,2
    ORDER BY 1,2
    """

REVOCATION_SESSIONS_TO_DATAFLOW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REVOCATION_SESSIONS_TO_DATAFLOW_VIEW_NAME,
    view_query_template=REVOCATION_SESSIONS_TO_DATAFLOW_QUERY_TEMPLATE,
    description=REVOCATION_SESSIONS_TO_DATAFLOW_DESCRIPTION,
    validation_views_dataset=dataset_config.VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_SESSIONS_TO_DATAFLOW_VIEW_BUILDER.build_and_print()
