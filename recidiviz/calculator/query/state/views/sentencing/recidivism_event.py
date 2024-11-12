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
Incarceration to state prison is the definition of a recidivism event for the purposes of Case Insights calculations.
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

NEW_SENTENCE_IMPOSED_CTE = """
    SELECT
      sigs.state_code,
      sigs.person_id,
      sigs.date_imposed as recidivism_date,
    FROM `{project_id}.sessions.sentence_imposed_group_summary_materialized` sigs
    JOIN `{project_id}.sessions.sentences_preprocessed_materialized` sp
      ON sigs.parent_sentence_id = sp.sentence_id
    WHERE JSON_EXTRACT_SCALAR(sp.sentence_metadata, "$.sentence_event_type") = 'INITIAL'
"""

RECIDIVISM_EVENT_QUERY_TEMPLATE = f"""
SELECT * FROM ({INCARCERATION_EVENT_CTE})

UNION ALL

SELECT * FROM ({NEW_SENTENCE_IMPOSED_CTE})
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
