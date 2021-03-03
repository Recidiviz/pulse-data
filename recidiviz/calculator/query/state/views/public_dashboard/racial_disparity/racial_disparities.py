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
"""Brings together various metric counts broken down by race/ethnicity for use on the 'Racial Disparities' page of
the public dashboard."""
# pylint: disable=trailing-whitespace
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RACIAL_DISPARITIES_VIEW_NAME = "racial_disparities"

RACIAL_DISPARITIES_VIEW_DESCRIPTION = (
    """Various metric counts broken down by race/ethnicity."""
)

RACIAL_DISPARITIES_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH state_race_ethnicity_groups AS (
      SELECT state_code,
             {state_specific_race_or_ethnicity_groupings},
             SUM(population_count) as total_state_population
      FROM `{project_id}.{static_reference_dataset}.state_race_ethnicity_population_counts` 
      GROUP BY state_code, race_or_ethnicity 
    ), sentenced_populations AS (
      SELECT
        state_code, race_or_ethnicity,
        incarceration_count as current_incarceration_sentence_count,
        probation_count as current_probation_sentence_count,
        dual_sentence_count as current_dual_sentence_count,
        total_population_count as current_total_sentenced_count
      FROM `{project_id}.{public_dashboard_dataset}.sentence_type_by_district_by_demographics` 
      WHERE (race_or_ethnicity != 'ALL' OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL'))
      AND district = 'ALL'
    ), incarcerated_population AS (
      SELECT state_code, race_or_ethnicity, population_count as total_incarcerated_population_36_mo
      FROM `{project_id}.{public_dashboard_dataset}.incarceration_population_by_prioritized_race_and_ethnicity_by_period` 
      WHERE metric_period_months = 36
    ), parole_population AS (
      SELECT state_code, race_or_ethnicity, total_supervision_population as total_parole_population_36_mo
      FROM `{project_id}.{public_dashboard_dataset}.supervision_population_by_prioritized_race_and_ethnicity_by_period`   
      WHERE metric_period_months = 36
      AND supervision_type = 'PAROLE'
    ), probation_population AS (
      SELECT state_code, race_or_ethnicity, total_supervision_population as total_probation_population_36_mo
      FROM `{project_id}.{public_dashboard_dataset}.supervision_population_by_prioritized_race_and_ethnicity_by_period`   
      WHERE metric_period_months = 36
      AND supervision_type = 'PROBATION'
    ), supervision_population AS (
      SELECT state_code, race_or_ethnicity, total_supervision_population as total_supervision_population_36_mo
      FROM `{project_id}.{public_dashboard_dataset}.supervision_population_by_prioritized_race_and_ethnicity_by_period`   
      WHERE metric_period_months = 36
      AND supervision_type = 'ALL'
    ), parole_revocations AS (
      SELECT state_code, race_or_ethnicity, new_crime_count AS parole_new_crime_count_36_mo,
        technical_count AS parole_technical_count_36_mo, absconsion_count as parole_absconsion_count_36_mo,
        unknown_count AS parole_unknown_count_36_mo
      FROM `{project_id}.{public_dashboard_dataset}.supervision_revocations_by_period_by_type_by_demographics` 
      WHERE (race_or_ethnicity != 'ALL' OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL'))
      AND supervision_type = 'PAROLE'
      AND metric_period_months = 36
    ), probation_revocations AS (
      SELECT state_code, race_or_ethnicity, new_crime_count AS probation_new_crime_count_36_mo,
        technical_count AS probation_technical_count_36_mo, absconsion_count as probation_absconsion_count_36_mo,
        unknown_count AS probation_unknown_count_36_mo
      FROM `{project_id}.{public_dashboard_dataset}.supervision_revocations_by_period_by_type_by_demographics` 
      WHERE (race_or_ethnicity != 'ALL' OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL'))
      AND supervision_type = 'PROBATION'
      AND metric_period_months = 36
    ), supervision_revocations AS (
      SELECT state_code, race_or_ethnicity, new_crime_count AS supervision_new_crime_count_36_mo,
        technical_count AS supervision_technical_count_36_mo, absconsion_count as supervision_absconsion_count_36_mo,
        unknown_count AS supervision_unknown_count_36_mo
      FROM `{project_id}.{public_dashboard_dataset}.supervision_revocations_by_period_by_type_by_demographics` 
      WHERE (race_or_ethnicity != 'ALL' OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL'))
      AND supervision_type = 'ALL'
      AND metric_period_months = 36
    ), parole_releases AS (
      SELECT state_code, race_or_ethnicity, parole_count as parole_release_count_36_mo
      FROM `{project_id}.{public_dashboard_dataset}.incarceration_releases_by_type_by_period` 
      WHERE (race_or_ethnicity != 'ALL' OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL'))
      AND metric_period_months = 36
    ), ftr_participants AS (
      SELECT state_code, race_or_ethnicity, participation_count as current_ftr_participation_count
      FROM `{project_id}.{public_dashboard_dataset}.active_program_participation_by_region`
      WHERE region_id = 'ALL'
      AND supervision_type = 'ALL'
    ), current_supervision_population AS (
      SELECT state_code, race_or_ethnicity, total_supervision_count as current_supervision_population
      FROM `{project_id}.{public_dashboard_dataset}.supervision_population_by_district_by_demographics`
      WHERE (race_or_ethnicity != 'ALL' OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL'))
      AND district = 'ALL'
      AND supervision_type = 'ALL'
    )
    
    SELECT * FROM
      state_race_ethnicity_groups
    LEFT JOIN
      sentenced_populations
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      incarcerated_population
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      parole_population
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      probation_population
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      supervision_population
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      parole_revocations
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      probation_revocations
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      supervision_revocations
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      parole_releases
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      ftr_participants
    USING (state_code, race_or_ethnicity)
    LEFT JOIN
      current_supervision_population
    USING (state_code, race_or_ethnicity)
    """

RACIAL_DISPARITIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=RACIAL_DISPARITIES_VIEW_NAME,
    view_query_template=RACIAL_DISPARITIES_VIEW_QUERY_TEMPLATE,
    dimensions=["state_code", "race_or_ethnicity"],
    description=RACIAL_DISPARITIES_VIEW_DESCRIPTION,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    public_dashboard_dataset=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RACIAL_DISPARITIES_VIEW_BUILDER.build_and_print()
