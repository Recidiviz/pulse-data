# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Event based supervision population for the most recent date of supervision."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.views.public_dashboard.utils import (
    spotlight_age_buckets,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_NAME = (
    "single_day_supervision_population_for_spotlight"
)

SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_DESCRIPTION = (
    """Event based supervision population for the most recent date of supervision."""
)


SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_QUERY_TEMPLATE = """
    SELECT
      pop.state_code,
      pop.person_id,
      pop.person_external_id,
      pop.case_type,
      {state_specific_supervision_type_groupings},
      pop.supervising_officer_external_id,
      pop.prioritized_race_or_ethnicity,
      e.projected_completion_date_max AS projected_end_date,
      IFNULL(pop.gender, 'EXTERNAL_UNKNOWN') as gender,
      {age_bucket},
      IFNULL(sent.judicial_district, 'EXTERNAL_UNKNOWN') as judicial_district_code,
      IFNULL(pop.supervising_district_external_id, 'EXTERNAL_UNKNOWN') as supervising_district_external_id,
      IFNULL(pop.supervision_level, 'EXTERNAL_UNKNOWN') as supervision_level,
      CURRENT_DATE('US/Eastern') AS date_of_supervision,
    FROM
      `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_span_metrics_materialized` pop
    LEFT JOIN `{project_id}.{sessions_dataset}.supervision_projected_completion_date_spans_materialized` e
        ON pop.state_code = e.state_code
        AND pop.person_id = e.person_id
        AND CURRENT_DATE('US/Eastern') BETWEEN e.start_date AND COALESCE(e.end_date, CURRENT_DATE('US/Eastern'))
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_closest_sentence_imposed_group` sent_map
        ON pop.state_code = sent_map.state_code
        AND pop.person_id = sent_map.person_id
        AND sent_map.end_date_exclusive IS NULL
    LEFT JOIN `{project_id}.{sessions_dataset}.sentence_imposed_group_summary_materialized` sent
        ON sent_map.state_code = sent.state_code
        AND sent_map.person_id = sent.person_id
        AND sent_map.sentence_imposed_group_id = sent.sentence_imposed_group_id
    WHERE pop.included_in_state_population
        AND pop.end_date_exclusive IS NULL
    """

SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_NAME,
    should_materialize=True,
    view_query_template=SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_QUERY_TEMPLATE,
    description=SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    age_bucket=spotlight_age_buckets("pop.age"),
    state_specific_supervision_type_groupings=state_specific_query_strings.state_specific_supervision_type_groupings(
        "pop"
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.build_and_print()
