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
"""Revocations by officer by metric period months."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.calculator.query import bqview, bq_utils
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

REVOCATIONS_BY_OFFICER_BY_PERIOD_VIEW_NAME = 'revocations_by_officer_by_period'

REVOCATIONS_BY_OFFICER_BY_PERIOD_DESCRIPTION = """
 Revocations by officer by metric period months.
 This counts all individuals admitted to prison for a revocation
 of probation or parole, broken down by the agent on the
 source_supervision_violation_response.
 """

# TODO(2231): Join against state_agent instead of temp_officers once state_agent
#  is properly ingested and entity-matched
REVOCATIONS_BY_OFFICER_BY_PERIOD_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code,
      IFNULL(felony_count, 0) AS felony_count,
      IFNULL(absconsion_count, 0) AS absconsion_count,
      IFNULL(technical_count, 0) AS technical_count,
      IFNULL(SAFE_SUBTRACT(all_violation_types_count, (felony_count + technical_count + absconsion_count)), 0) AS unknown_count,
      total_supervision_count,
      supervision_type,
      officer_external_id,
      district,
      metric_period_months
    FROM (
      SELECT
        state_code,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        district,
        officer_external_id,
        metric_period_months
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, metric_period_months, supervision_type, district, officer_external_id
    ) pop
    LEFT JOIN (
      -- Aggregate revocations per officer/district --
      SELECT
        state_code,
        supervision_type,
        COUNT(DISTINCT IF(source_violation_type = 'FELONY', person_id, NULL)) AS felony_count,
        COUNT(DISTINCT IF(source_violation_type = 'TECHNICAL', person_id, NULL)) AS technical_count,
        COUNT(DISTINCT IF(source_violation_type = 'ABSCONDED', person_id, NULL)) AS absconsion_count,
        COUNT(DISTINCT person_id) AS all_violation_types_count,
        district,
        officer_external_id,
        metric_period_months
      FROM (
        SELECT
          state_code, metric_period_months,
          person_id, source_violation_type,
          supervision_type, district, officer_external_id,
          -- Only use most recent revocation per person/supervision_type/metric_period_months
          ROW_NUMBER() OVER (PARTITION BY state_code, person_id, supervision_type, metric_period_months, district,
                             officer_external_id ORDER BY revocation_admission_date DESC) AS revocation_rank
        FROM `{project_id}.{reference_dataset}.event_based_revocations`,
        {metric_period_dimension}
        WHERE {metric_period_condition}
      )
      WHERE revocation_rank = 1
      GROUP BY state_code, metric_period_months, supervision_type, district, officer_external_id
    ) rev
    USING (state_code, supervision_type, district, officer_external_id, metric_period_months)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
    ORDER BY state_code, officer_external_id, district, supervision_type, metric_period_months
    """.format(
        description=REVOCATIONS_BY_OFFICER_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
        metric_period_dimension=bq_utils.unnest_metric_period_months(),
        metric_period_condition=bq_utils.metric_period_condition(),
    )

REVOCATIONS_BY_OFFICER_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_OFFICER_BY_PERIOD_VIEW_NAME,
    view_query=REVOCATIONS_BY_OFFICER_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_OFFICER_BY_PERIOD_VIEW.view_id)
    print(REVOCATIONS_BY_OFFICER_BY_PERIOD_VIEW.view_query)
