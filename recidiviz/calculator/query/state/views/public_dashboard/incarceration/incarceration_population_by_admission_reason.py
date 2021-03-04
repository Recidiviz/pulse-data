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
"""Most recent daily incarceration population count broken down by reason for admission and demographics."""
# pylint: disable=trailing-whitespace
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_NAME = (
    "incarceration_population_by_admission_reason"
)

INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_DESCRIPTION = """Most recent daily incarceration population count broken down by reason for admission and demographics."""

INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH state_specific_groupings AS (
      SELECT 
        person_id,
        state_code,
        date_of_stay,
        gender,
        age_bucket,
        admission_reason,
        {state_specific_race_or_ethnicity_groupings}
      FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_daily_incarceration_population_materialized`
    )
    
    SELECT
      state_code,
      date_of_stay,
      race_or_ethnicity,
      gender,
      age_bucket,
      COUNT(DISTINCT IF(admission_reason = 'NEW_ADMISSION', person_id, NULL)) as new_admission_count,
      COUNT(DISTINCT IF(admission_reason = 'PAROLE_REVOCATION', person_id, NULL)) as parole_revocation_count,
      COUNT(DISTINCT IF(admission_reason = 'PROBATION_REVOCATION', person_id, NULL)) as probation_revocation_count,
      COUNT(DISTINCT IF(admission_reason NOT IN ('NEW_ADMISSION', 'PAROLE_REVOCATION', 'PROBATION_REVOCATION'), person_id, NULL)) as other_count,
      COUNT(DISTINCT(person_id)) as total_population
    FROM
      state_specific_groupings,
      {unnested_race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension}
    WHERE (race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- State-wide count
    GROUP BY state_code, date_of_stay,  race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, date_of_stay, race_or_ethnicity, gender, age_bucket
    """

INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "date_of_stay",
        "race_or_ethnicity",
        "gender",
        "age_bucket",
    ),
    description=INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column(
        "race_or_ethnicity", "race_or_ethnicity"
    ),
    gender_dimension=bq_utils.unnest_column("gender", "gender"),
    age_dimension=bq_utils.unnest_column("age_bucket", "age_bucket"),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(
        "prioritized_race_or_ethnicity"
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_BUILDER.build_and_print()
