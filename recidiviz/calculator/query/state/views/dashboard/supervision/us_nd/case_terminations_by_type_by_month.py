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
"""Case Terminations by type by month."""

from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW_NAME = "case_terminations_by_type_by_month"

CASE_TERMINATIONS_BY_TYPE_BY_MONTH_DESCRIPTION = """
Supervision period termination count split by termination reason, month, district, and supervision type.
"""


# TODO(#4155): Use the supervision_termination_metrics instead of the raw state_supervision_period table
def _get_query_prep_statement(reference_views_dataset: str) -> str:
    """Return the Common Table Expression used to gather the termination case data"""
    return f"""
        -- Gather supervision period case termination data
        WITH case_terminations AS (
          SELECT
            supervision_period.state_code,
            EXTRACT(YEAR FROM termination_date) AS year,
            EXTRACT(MONTH FROM termination_date) AS month,
            supervision_period.termination_reason,
            supervision_period.person_id,
            supervision_type,
            IFNULL(district, 'EXTERNAL_UNKNOWN') as district,
            IFNULL(agent.agent_external_id, 'EXTERNAL_UNKNOWN') AS officer_external_id
          FROM `{{project_id}}.state.state_supervision_period` supervision_period
          LEFT JOIN `{{project_id}}.{reference_views_dataset}.supervision_period_to_agent_association` agent
            USING (supervision_period_id),
          {bq_utils.unnest_district(district_column="supervision_site")},
          {bq_utils.unnest_supervision_type(supervision_type_column="supervision_period.supervision_type")}
          WHERE termination_date IS NOT NULL
        )

    """


CASE_TERMINATIONS_BY_TYPE_BY_MONTH_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    {_get_query_prep_statement(reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET)}
    SELECT
      state_code, year, month,
      COUNT(DISTINCT absconsion) AS absconsion,
      COUNT(DISTINCT death) AS death,
      COUNT(DISTINCT discharge) AS discharge,
      COUNT(DISTINCT expiration) AS expiration,
      COUNT(DISTINCT revocation) AS revocation,
      COUNT(DISTINCT suspension) AS suspension,
      COUNT(DISTINCT other) AS other,
      supervision_type,
      district
    FROM (
      SELECT
        state_code, year, month,
        CASE WHEN termination_reason = 'ABSCONSION' THEN person_id ELSE NULL END AS absconsion,
        CASE WHEN termination_reason = 'DEATH' THEN person_id ELSE NULL END AS death,
        CASE WHEN termination_reason = 'DISCHARGE' THEN person_id ELSE NULL END AS discharge,
        CASE WHEN termination_reason = 'EXPIRATION' THEN person_id ELSE NULL END AS expiration,
        CASE WHEN termination_reason = 'REVOCATION' THEN person_id ELSE NULL END AS revocation,
        CASE WHEN termination_reason = 'SUSPENSION' THEN person_id ELSE NULL END AS suspension,
        CASE WHEN termination_reason = 'EXTERNAL_UNKNOWN' THEN person_id ELSE NULL END AS other,
        supervision_type,
        district
      FROM case_terminations
    )
    WHERE supervision_type IN ('ALL', 'PROBATION', 'PAROLE')
      AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 3 YEAR))
    GROUP BY state_code, year, month, supervision_type, district
    ORDER BY state_code, year, month, supervision_type, district
    """

CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query_template=CASE_TERMINATIONS_BY_TYPE_BY_MONTH_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", "supervision_type", "district"),
    description=CASE_TERMINATIONS_BY_TYPE_BY_MONTH_DESCRIPTION,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW_BUILDER.build_and_print()
