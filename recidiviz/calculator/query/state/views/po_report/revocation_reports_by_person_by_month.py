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
"""Revocation report recommendations by person by month."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.po_report.violation_reports_query import violation_reports_query
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REVOCATION_REPORTS_BY_PERSON_BY_MONTH_VIEW_NAME = \
    'revocation_reports_by_person_by_month'

REVOCATION_REPORTS_BY_PERSON_BY_MONTH_DESCRIPTION = """
    Revocation report recommendations by person by month. If multiple violation reports recommend revocation in the 
    month, we filter for the most severe violation type (i.e. "new crime" violation types over "technical") 
"""

REVOCATION_REPORTS_BY_PERSON_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH violation_reports AS (
      {violation_reports_query}
    ),
    revocation_recommendations AS (
    SELECT
      state_code, year, month, officer_external_id, person_id, response_date,
      IF(violation_type IN ('FELONY', 'MISDEMEANOR', 'LAW'), 'NEW_CRIME', violation_type) AS violation_type
    FROM violation_reports
    WHERE violation_type IN ('FELONY', 'MISDEMEANOR', 'LAW', 'TECHNICAL')
      AND response_decision IN ('REVOCATION')
    ),
    revocation_recommendations_ranking AS (
      SELECT
        state_code, year, month, officer_external_id, person_id, violation_type, response_date,
        RANK() OVER(PARTITION BY person_id, year, month, officer_external_id ORDER BY (
        CASE
            WHEN violation_type IN ('NEW_CRIME') THEN 1
            WHEN violation_type in ('TECHNICAL') THEN 2
            ELSE 3
        END)
      ) AS revocation_violation_type_rank
      FROM revocation_recommendations
    )
    SELECT DISTINCT
        state_code, year, month, person_id, officer_external_id, 
        violation_type,
        response_date AS revocation_report_date
      FROM revocation_recommendations_ranking
      WHERE revocation_violation_type_rank = 1
    """

REVOCATION_REPORTS_BY_PERSON_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=REVOCATION_REPORTS_BY_PERSON_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=REVOCATION_REPORTS_BY_PERSON_BY_MONTH_QUERY_TEMPLATE,
    description=REVOCATION_REPORTS_BY_PERSON_BY_MONTH_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    state_dataset=dataset_config.STATE_BASE_DATASET,
    district_dimension=bq_utils.unnest_district(district_column='district'),
    po_report_dataset=dataset_config.PO_REPORT_DATASET,
    violation_reports_query=violation_reports_query(
        state_dataset=dataset_config.STATE_BASE_DATASET,
        reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET
    )
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        REVOCATION_REPORTS_BY_PERSON_BY_MONTH_VIEW_BUILDER.build_and_print()
