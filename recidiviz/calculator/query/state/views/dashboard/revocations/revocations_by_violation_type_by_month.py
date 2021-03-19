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
"""Revocations by violation type by month."""
# pylint: disable=trailing-whitespace

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_NAME = (
    "revocations_by_violation_type_by_month"
)

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_DESCRIPTION = (
    """ Revocations by violation type by month """
)

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        state_code, year, month,
        IFNULL(felony_count, 0) AS felony_count,
        IFNULL(absconsion_count, 0) AS absconsion_count,
        IFNULL(technical_count, 0) AS technical_count,
        IFNULL(SAFE_SUBTRACT(all_violation_types_count, (felony_count + technical_count + absconsion_count)), 0) AS unknown_count,
        total_supervision_count,
        supervision_type,
        district
    FROM (
      SELECT
        state_code, year, month,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        district
      FROM `{project_id}.{reference_views_dataset}.event_based_supervision_populations`
      GROUP BY state_code, year, month, supervision_type, district
    ) pop
    LEFT JOIN (
      SELECT
        state_code, year, month,
        COUNT(DISTINCT IF(most_severe_violation_type IN ('FELONY', 'MISDEMEANOR', 'LAW'), person_id, NULL)) AS felony_count,
        COUNT(DISTINCT IF(most_severe_violation_type = 'TECHNICAL', person_id, NULL)) AS technical_count,
        COUNT(DISTINCT IF(most_severe_violation_type IN ('ESCAPED', 'ABSCONDED'), person_id, NULL)) AS absconsion_count,
        COUNT(DISTINCT person_id) AS all_violation_types_count,
        supervision_type,
        district
      FROM (
        SELECT
          state_code, year, month,
          person_id, most_severe_violation_type,
          supervision_type, district,
          -- Only use most recent revocation per person/supervision_type/metric_period_months
          ROW_NUMBER() OVER (PARTITION BY state_code, year, month, person_id, supervision_type, district
                             ORDER BY revocation_admission_date DESC) AS revocation_rank
        FROM `{project_id}.{reference_views_dataset}.event_based_revocations`
      )
      WHERE revocation_rank = 1
      GROUP BY state_code, year, month, supervision_type, district
    ) rev
    USING (state_code, year, month, supervision_type, district)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
    ORDER BY state_code, year, month, district, supervision_type
    """

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_NAME,
    view_query_template=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", "district", "supervision_type"),
    description=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_BUILDER.build_and_print()
