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
"""Incarceration facility population by purpose by day."""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder, SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET, DATAFLOW_METRICS_MATERIALIZED_DATASET, \
    STATIC_REFERENCE_TABLES_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_POPULATION_BY_PURPOSE_BY_DAY_VIEW_NAME = 'incarceration_population_by_purpose_by_day'

INCARCERATION_POPULATION_BY_PURPOSE_BY_DAY_DESCRIPTION = \
    """ Incarceration facility population by purpose by day."""

INCARCERATION_POPULATION_BY_PURPOSE_BY_DAY_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH daily_population AS (
      SELECT
        state_code,
        person_id,
        IFNULL(facility_shorthand, facility) as facility,
        date_of_stay,
        specialized_purpose_for_incarceration,
      FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_materialized`
      LEFT JOIN
        `{project_id}.{static_reference_dataset}.state_incarceration_facility_capacity`
      USING (state_code, facility)
      WHERE methodology = 'EVENT'
      -- Revisit these exclusions when #3657 and #3723 are complete --
      AND (state_code != 'US_ND' OR facility not in ('OOS', 'CPP'))
      AND EXTRACT(YEAR FROM date_of_stay) > EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR))
    )
    
    SELECT
      state_code,
      facility,
      date_of_stay,
      COUNT(DISTINCT IF(specialized_purpose_for_incarceration = 'PAROLE_BOARD_HOLD', person_id, NULL)) AS 
        parole_board_hold_count,
      COUNT(DISTINCT(person_id)) as total_population
    FROM
      daily_population,
      {facility_dimension}
    GROUP BY state_code, facility, date_of_stay
    ORDER BY state_code, facility, date_of_stay
"""

INCARCERATION_POPULATION_BY_PURPOSE_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    view_id=INCARCERATION_POPULATION_BY_PURPOSE_BY_DAY_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_PURPOSE_BY_DAY_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_BY_PURPOSE_BY_DAY_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    facility_dimension=bq_utils.unnest_column('facility', 'facility'),
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_PURPOSE_BY_DAY_VIEW_BUILDER.build_and_print()
