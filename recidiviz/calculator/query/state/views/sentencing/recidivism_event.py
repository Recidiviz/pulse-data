#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""View of events that can count as recidivism."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RECIDIVISM_EVENT_VIEW_NAME = "recidivism_event"

RECIDIVISM_EVENT_DESCRIPTION = """
Events that can count as recidivism -- either INCARCERATION, or PROBATION with inflow from LIBERTY.
"""

RECIDIVISM_EVENT_QUERY_TEMPLATE = """
    SELECT
      person_id,
      state_code,
      start_date AS recidivism_date,
      compartment_level_2
    FROM `{project_id}.sessions.compartment_sessions_materialized`
    WHERE
        (compartment_level_1 = 'INCARCERATION'
         AND compartment_level_2 = 'GENERAL')
        OR
        (compartment_level_1 = 'SUPERVISION'
         AND compartment_level_2 = 'PROBATION'
         AND inflow_from_level_1 = 'LIBERTY')
    """


RECIDIVISM_EVENT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=RECIDIVISM_EVENT_VIEW_NAME,
    dataset_id=dataset_config.SENTENCING_OUTPUT_DATASET,
    view_query_template=RECIDIVISM_EVENT_QUERY_TEMPLATE,
    description=RECIDIVISM_EVENT_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RECIDIVISM_EVENT_VIEW_BUILDER.build_and_print()
