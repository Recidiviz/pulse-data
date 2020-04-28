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

from recidiviz.calculator.query import bqview, bq_utils
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata
from recidiviz.calculator.query.state.views.supervision.us_nd.case_terminations_by_type_by_month import \
    _get_query_prep_statement

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW_NAME = 'case_terminations_by_type_by_officer_by_period'

CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_DESCRIPTION = """
    Supervision period termination count split by termination reason, terminating officer, district, supervision type,
    and metric period months (1, 3, 6, 12, 36).
"""

CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_QUERY = \
    """
    /*{description}*/
    {prep_expression}
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
      officer_external_id,
      district, 
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
      {metric_period_dimension}
      WHERE {metric_period_condition}
    )
    WHERE supervision_type IN ('ALL', 'PROBATION', 'PAROLE')
      AND district != 'ALL'
    GROUP BY state_code, metric_period_months, supervision_type, officer_external_id, district
    ORDER BY state_code, supervision_type, district, officer_external_id, metric_period_months
    """.format(
        description=CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_DESCRIPTION,
        prep_expression=_get_query_prep_statement(project_id=PROJECT_ID, reference_dataset=REFERENCE_DATASET),
        metric_period_dimension=bq_utils.unnest_metric_period_months(),
        metric_period_condition=bq_utils.metric_period_condition(),
    )

CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW_NAME,
    view_query=CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW.view_id)
    print(CASE_TERMINATIONS_BY_TYPE_BY_OFFICER_BY_PERIOD_VIEW.view_query)
