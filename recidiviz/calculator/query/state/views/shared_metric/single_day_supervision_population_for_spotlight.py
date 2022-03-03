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
from recidiviz.calculator.query.state import dataset_config
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
    /*{description}*/
    SELECT
      state_code,
      person_id,
      person_external_id,
      case_type,
      supervision_type,
      supervising_officer_external_id,
      prioritized_race_or_ethnicity,
      projected_end_date,
      IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
      {age_bucket},
      IFNULL(judicial_district_code, 'EXTERNAL_UNKNOWN') as judicial_district_code,
      IFNULL(supervising_district_external_id, 'EXTERNAL_UNKNOWN') as supervising_district_external_id,
      IFNULL(supervision_level, 'EXTERNAL_UNKNOWN') as supervision_level,
      date_of_supervision
    FROM
      `{project_id}.{materialized_metrics_dataset}.most_recent_single_day_supervision_population_metrics_materialized`
    """

SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_NAME,
    should_materialize=True,
    view_query_template=SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_QUERY_TEMPLATE,
    description=SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    age_bucket=spotlight_age_buckets(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.build_and_print()
