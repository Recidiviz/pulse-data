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
from recidiviz.calculator.query.state.views.sentencing.us_ix.sentencing_recidivism_event_template import (
    US_IX_SENTENCING_RECIDIVISM_EVENT_TEMPLATE,
)
from recidiviz.calculator.query.state.views.sentencing.us_nd.sentencing_recidivism_event_template import (
    US_ND_SENTENCING_RECIDIVISM_EVENT_TEMPLATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RECIDIVISM_EVENT_VIEW_NAME = "recidivism_event"

RECIDIVISM_EVENT_DESCRIPTION = """
Incarceration to state prison is the definition of a recidivism event for the purposes of Case Insights calculations.

This combines state-specific sentencing queries with a state-agnostic query for actual incarceration events to serve as
a backstop in case an incarceration sentence is missed in state-specific logic. 
"""


# TODO(#32123): Use state prison incarceration sessions as data source to reduce event counts
INCARCERATION_EVENT_CTE = """
    SELECT
      state_code,
      person_id,
      start_date_inclusive as recidivism_date,
    FROM `{project_id}.dataflow_metrics_materialized.most_recent_incarceration_population_span_metrics_materialized`
    WHERE incarceration_type = "STATE_PRISON"
"""


RECIDIVISM_EVENT_QUERY_TEMPLATE = f"""
SELECT * FROM ({US_IX_SENTENCING_RECIDIVISM_EVENT_TEMPLATE})

UNION ALL

SELECT * FROM ({US_ND_SENTENCING_RECIDIVISM_EVENT_TEMPLATE})

UNION ALL

SELECT * FROM ({INCARCERATION_EVENT_CTE})
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
