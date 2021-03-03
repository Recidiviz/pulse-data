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
"""Case Terminations by type by officer by period."""
# pylint: disable=trailing-whitespace

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.supervision.us_nd.case_terminations_by_type_by_month import (
    _get_query_prep_statement,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW_NAME = (
    "case_terminations_by_type_by_officer_by_period"
)

CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_DESCRIPTION = """
    Supervision period termination count split by termination reason, terminating officer, district, supervision type,
    and metric period months (1, 3, 6, 12, 36).
"""

CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    {_get_query_prep_statement(reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET)}
    SELECT
      state_code,
      COUNT(DISTINCT absconsion) AS absconsion,
      COUNT(DISTINCT death) AS death,
      COUNT(DISTINCT discharge) AS discharge,
      COUNT(DISTINCT expiration) AS expiration,
      COUNT(DISTINCT revocation) AS revocation,
      COUNT(DISTINCT suspension) AS suspension,
      COUNT(DISTINCT other) AS other,
      supervision_type,
      IFNULL(officer_external_id, 'EXTERNAL_UNKNOWN') as officer_external_id,
      IFNULL(district, 'EXTERNAL_UNKNOWN') as district, 
      metric_period_months
    FROM (
      SELECT
        state_code,
        CASE WHEN termination_reason = 'ABSCONSION' THEN person_id ELSE NULL END AS absconsion,
        CASE WHEN termination_reason = 'DEATH' THEN person_id ELSE NULL END AS death,
        CASE WHEN termination_reason = 'DISCHARGE' THEN person_id ELSE NULL END AS discharge,
        CASE WHEN termination_reason = 'EXPIRATION' THEN person_id ELSE NULL END AS expiration,
        CASE WHEN termination_reason = 'REVOCATION' THEN person_id ELSE NULL END AS revocation,
        CASE WHEN termination_reason = 'SUSPENSION' THEN person_id ELSE NULL END AS suspension,
        CASE WHEN termination_reason = 'EXTERNAL_UNKNOWN' THEN person_id ELSE NULL END AS other,
        supervision_type,
        officer_external_id,
        district, 
        metric_period_months
      FROM case_terminations,
      {{metric_period_dimension}}
      WHERE {{metric_period_condition}}
    )
    WHERE supervision_type IN ('ALL', 'PROBATION', 'PAROLE')
      AND district != 'ALL'
    GROUP BY state_code, metric_period_months, supervision_type, officer_external_id, district
    ORDER BY state_code, supervision_type, district, officer_external_id, metric_period_months
    """

CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW_NAME,
    view_query_template=CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_QUERY_TEMPLATE,
    dimensions=[
        "state_code",
        "metric_period_months",
        "supervision_type",
        "district",
        "officer_external_id",
    ],
    description=CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_DESCRIPTION,
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    metric_period_condition=bq_utils.metric_period_condition(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW_BUILDER.build_and_print()
