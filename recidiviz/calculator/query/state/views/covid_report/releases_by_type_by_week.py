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
"""Releases from incarceration by week, broken down by total releases and conditional releases"""
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RELEASES_BY_TYPE_BY_WEEK_VIEW_NAME = 'releases_by_type_by_week'

RELEASES_BY_TYPE_BY_WEEK_DESCRIPTION = \
    """ Releases from incarceration by week, broken down by total releases and conditional releases """

RELEASES_BY_TYPE_BY_WEEK_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH releases AS (
        SELECT
          job_id,
          report.state_code,
          week_num,
          start_date,
          end_date,
          person_id,
          release_date,
          release_reason,
          supervision_type_at_release 
        FROM
          `{project_id}.{reference_views_dataset}.covid_report_weeks` report
        JOIN
          `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics` releases
        USING (state_code)
        WHERE releases.release_date BETWEEN report.start_date AND report.end_date
          AND release_reason in ('COMMUTED', 'COMPASSIONATE', 'CONDITIONAL_RELEASE', 'SENTENCE_SERVED')
          AND methodology = 'EVENT'
          AND metric_period_months = 1
    )
    , releases_by_week AS (
      SELECT
        state_code,
        week_num,
        start_date,
        end_date,
        COUNT(DISTINCT(person_id)) as total_release_count,
        COUNT(DISTINCT IF(supervision_type_at_release = 'PAROLE', person_id, NULL)) as release_to_parole_count
      FROM releases
      GROUP BY state_code, week_num, start_date, end_date
    )
    SELECT
      state_code,
      week_num,
      start_date,
      end_date,
      total_release_count,
      IFNULL(total_release_count - LAG(total_release_count) OVER (PARTITION BY state_code ORDER BY week_num), 0) as total_release_count_diff,
      release_to_parole_count,
      IFNULL(release_to_parole_count - LAG(release_to_parole_count) OVER (PARTITION BY state_code ORDER BY week_num), 0) as release_to_parole_count_diff,
    FROM (
      SELECT
        state_code, week_num, start_date, end_date,
        (IFNULL(total_release_count, 0) + IFNULL(admission_count, 0)) as total_release_count,
        IFNULL(release_to_parole_count, 0) AS release_to_parole_count
      FROM
        releases_by_week
      FULL OUTER JOIN
        `{project_id}.{covid_report_dataset}.admissions_to_cpp_by_week`
      USING (state_code, week_num, start_date, end_date)
    )
    ORDER BY state_code, week_num
"""

RELEASES_BY_TYPE_BY_WEEK_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    view_id=RELEASES_BY_TYPE_BY_WEEK_VIEW_NAME,
    view_query_template=RELEASES_BY_TYPE_BY_WEEK_QUERY_TEMPLATE,
    description=RELEASES_BY_TYPE_BY_WEEK_DESCRIPTION,
    covid_report_dataset=dataset_config.COVID_REPORT_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        RELEASES_BY_TYPE_BY_WEEK_VIEW_BUILDER.build_and_print()
