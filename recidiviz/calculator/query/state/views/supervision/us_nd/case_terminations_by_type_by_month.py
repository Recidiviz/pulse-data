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
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW_NAME = 'case_terminations_by_type_by_month'

CASE_TERMINATIONS_BY_TYPE_BY_MONTH_DESCRIPTION = """
    Supervision period termination count split by termination reason, month, district, and supervision type.
"""


def _get_query_prep_statement(project_id, reference_dataset):
    """Return the Common Table Expression used to gather the termination case data"""
    return """
        -- Gather supervision period case termination data
        WITH case_terminations AS (
          SELECT
            supervision_period.state_code,
            EXTRACT(YEAR FROM termination_date) AS year,
            EXTRACT(MONTH FROM termination_date) AS month,
            supervision_period.termination_reason,
            supervision_period.person_id,
            supervision_type,
            district,
            agent.agent_external_id AS officer_external_id
          FROM `{project_id}.state.state_supervision_period` supervision_period
          LEFT JOIN `{project_id}.{reference_dataset}.supervision_period_to_agent_association` agent
            USING (supervision_period_id),
          {district_dimension},
          {supervision_dimension}
          WHERE termination_date IS NOT NULL
        )
    """.format(
        project_id=project_id,
        reference_dataset=reference_dataset,
        district_dimension=bq_utils.unnest_district(district_column='agent.district_external_id'),
        supervision_dimension=
        bq_utils.unnest_supervision_type(supervision_type_column='supervision_period.supervision_type'),
    )


CASE_TERMINATIONS_BY_TYPE_BY_MONTH_QUERY = \
    """
    /*{description}*/
    {prep_expression}
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
    """.format(
        description=CASE_TERMINATIONS_BY_TYPE_BY_MONTH_DESCRIPTION,
        prep_expression=_get_query_prep_statement(project_id=PROJECT_ID, reference_dataset=REFERENCE_DATASET)
    )

CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW = BigQueryView(
    view_id=CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query=CASE_TERMINATIONS_BY_TYPE_BY_MONTH_QUERY
)


if __name__ == '__main__':
    print(CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW.view_id)
    print(CASE_TERMINATIONS_BY_TYPE_BY_MONTH_VIEW.view_query)
