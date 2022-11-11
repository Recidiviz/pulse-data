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
"""Event based incarceration population for the most recent date of incarceration."""
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

SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_NAME = (
    "single_day_incarceration_population_for_spotlight"
)

SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_DESCRIPTION = """Event based incarceration population for the most recent date of incarceration."""


SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_QUERY_TEMPLATE = """
    SELECT
      pop.state_code,
      pop.person_id,
      sess.start_reason AS admission_reason,
      pop.prioritized_race_or_ethnicity,
      IFNULL(pop.gender, 'EXTERNAL_UNKNOWN') as gender,
      {age_bucket},
      IFNULL(judicial_district_code, 'EXTERNAL_UNKNOWN') as judicial_district_code,
      CURRENT_DATE('US/Eastern') AS date_of_stay,
      pop.facility,
      inc.supervision_type AS commitment_from_supervision_supervision_type
    FROM
      (SELECT * FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_span_metrics_materialized`
        WHERE included_in_state_population and end_date_exclusive IS NULL) pop
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` sess
        ON CURRENT_DATE('US/Eastern') BETWEEN sess.start_date AND COALESCE(sess.end_date, CURRENT_DATE('US/Eastern'))
        AND pop.person_id = sess.person_id
    LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized` inc
        ON sess.person_id = inc.person_id
        AND sess.start_date = inc.admission_date
    WHERE {state_specific_facility_exclusion}
    """

SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_NAME,
    should_materialize=True,
    view_query_template=SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_QUERY_TEMPLATE,
    description=SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    state_specific_facility_exclusion=state_specific_query_strings.state_specific_facility_exclusion(
        "pop"
    ),
    age_bucket=spotlight_age_buckets("pop.age"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.build_and_print()
