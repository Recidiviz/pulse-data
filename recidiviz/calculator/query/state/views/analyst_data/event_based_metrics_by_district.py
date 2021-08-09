# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""View tracking daily positive and backstop metrics at the district level
for use in impact measurement"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_METRICS_BY_DISTRICT_VIEW_NAME = "event_based_metrics_by_district"

EVENT_BASED_METRICS_BY_DISTRICT_VIEW_DESCRIPTION = "Reports metrics counting events at district-level at various time scales on a daily cadence"

EVENT_BASED_METRICS_BY_DISTRICT_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT state_code, date, primary_district as district, rolling_window_days,
        SUM(supervision_population) AS supervision_population,
        SUM(num_successful_completions) AS num_successful_completions,
        SUM(num_successful_completions)*100/SUM(supervision_population) AS num_successful_completions_per_100,
        SUM(num_earned_discharge_requests) AS num_earned_discharge_requests,
        SUM(num_earned_discharge_requests)*100/SUM(supervision_population) AS num_earned_discharge_requests_per_100,
        SUM(num_supervision_downgrades) AS num_supervision_downgrades,
        SUM(num_supervision_downgrades)*100/SUM(supervision_population) AS num_supervision_downgrades_per_100,
        SUM(num_technical_or_absconsion_revocations) AS num_technical_or_absconsion_revocations,
        SUM(num_technical_or_absconsion_revocations)*100/SUM(supervision_population) AS num_technical_or_absconsion_revocations_per_100,
        SUM(num_new_crime_revocations) AS num_new_crime_revocations,
        SUM(num_new_crime_revocations)*100/SUM(supervision_population) AS num_new_crime_revocations_per_100,
        SUM(num_rider_revocations) AS num_rider_revocations,
        SUM(num_rider_revocations)*100/SUM(supervision_population) AS num_rider_revocations_per_100,
        SUM(num_parole_board_hold_revocations) AS num_parole_board_hold_revocations,
        SUM(num_parole_board_hold_revocations)*100/SUM(supervision_population) AS num_parole_board_hold_revocations_per_100,
        SUM(num_absconsions) AS num_absconsions,
        SUM(num_absconsions)*100/SUM(supervision_population) AS num_absconsions_per_100,
        SUM(num_violations) AS num_violations,
        SUM(num_violations)*100/SUM(supervision_population) AS num_violations_per_100,
    /* Aggregates from officer-level view based on the primary district of the officer */
    FROM `{project_id}.{analyst_dataset}.event_based_metrics_by_supervision_officer_materialized`
    GROUP BY state_code, date, primary_district, rolling_window_days
    """

EVENT_BASED_METRICS_BY_DISTRICT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=EVENT_BASED_METRICS_BY_DISTRICT_VIEW_NAME,
    view_query_template=EVENT_BASED_METRICS_BY_DISTRICT_QUERY_TEMPLATE,
    description=EVENT_BASED_METRICS_BY_DISTRICT_VIEW_DESCRIPTION,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_METRICS_BY_DISTRICT_VIEW_BUILDER.build_and_print()
