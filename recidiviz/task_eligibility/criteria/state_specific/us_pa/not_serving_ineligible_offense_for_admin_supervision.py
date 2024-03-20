# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
Defines a criteria view that shows spans of time when clients are not serving any Admin Supervision-ineligible sentences
"""
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_PA_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION"

_DESCRIPTION = """Defines a criteria view that shows spans of time when clients are not serving any
 Admin Supervision-ineligible sentences"""

_REASON_QUERY = """
    WITH ineligible_spans AS (
          SELECT
          span.state_code,
          span.person_id,
          span.start_date,
          span.end_date,
          sent.projected_completion_date_max,
          sc.description,
          FALSE as meets_criteria,
          FROM
          `{project_id}.{sessions_dataset}.sentence_spans_materialized` span,
            UNNEST (sentences_preprocessed_id_array_actual_completion) AS sentences_preprocessed_id
          INNER JOIN
            `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sent
          USING
            (state_code,
              person_id,
              sentences_preprocessed_id)
          LEFT JOIN
            `{project_id}.{normalized_state_dataset}.state_charge` sc
          ON
            span.person_id = sc.person_id
            AND span.state_code = sc.state_code
            AND sent.charge_id = sc.charge_id
          WHERE
            span.state_code = 'US_PA'
            AND ((sc.statute LIKE 'CC25%' AND sc.description LIKE '%CRIMINAL HOMICIDE%')
                OR (sc.statute LIKE 'CC27%' AND sc.offense_type LIKE '%ASSAULT%')
                OR (sc.statute LIKE 'CC29%' AND sc.offense_type LIKE '%KIDNAP%')
                OR (sc.statute LIKE 'CC31%' AND sc.description LIKE '%SEXUAL ASSAULT%')
                OR (sc.statute LIKE 'CC33%' AND sc.description LIKE '%ARSON%')
                OR (sc.statute LIKE 'CC37%' AND sc.offense_type LIKE '%ROBBERY%')
                OR (sc.statute LIKE 'CC49%' AND sc.description LIKE '%INTIMIDATION%')
                -- Relating to homicide via watercraft while under the influence
                OR (sc.statute LIKE 'VC5502.1')
                -- Relating to DUI/Controlled Substance in cases involving bodily injury
                OR (sc.statute LIKE 'VC3731%')
                -- Relating to homicide via vehicle
                OR (sc.statute LIKE 'VC3732%')
                OR (sc.statute IN ('VC3735','VC3735A'))
                OR (sc.statute LIKE 'VC3735.1')
                -- Relating to accidents involving death or personal injury
                OR (sc.statute LIKE 'VC3742%')
                -- Incest
                OR (sc.statute LIKE '%4302%')
                -- Open lewdness
                OR (sc.statute LIKE 'CC5901')
                -- Prostitution
                OR (sc.statute IN ('CC5902B', 'CS5902B', 'CC5902B1', 'CC5902B3', 'CC5902B4'))
                -- Sexual performance where victim is a minor
                OR (sc.statute LIKE 'CC5903%')
                -- Internet Child Pornography
                OR (sc.statute LIKE 'CC76%')
                -- Megan's Law Registration
                OR (sc.statute LIKE '%9795%')
                -- Sexual Abuse of Children
                OR ((sc.statute LIKE 'CC6312%' OR sc.statute LIKE 'CS6312'))
                -- Unlawful Contact with Minor
                OR (sc.statute LIKE 'CC6318%')
                -- Sexual Abuse of Children
                OR (sc.statute LIKE 'CC6320%')
                )
          -- Removes a few cases where someone has multiple spans for the same offense so they aren't later 
          -- aggregated unnecessarily 
          QUALIFY ROW_NUMBER() OVER (PARTITION BY span.person_id, span.start_date, sc.description ORDER BY
          projected_completion_date_max DESC) = 1
    )
    SELECT 
      state_code,
      person_id,
      start_date,
      end_date,
      meets_criteria,
      TO_JSON(STRUCT(
                ARRAY_AGG(description ORDER BY COALESCE(projected_completion_date_max,'9999-01-01')) AS ineligible_offenses,
                ARRAY_AGG(projected_completion_date_max ORDER BY COALESCE(projected_completion_date_max,'9999-01-01')) AS ineligible_sentences_expiration_date
                )) AS reason,
      FROM ineligible_spans
      GROUP BY 1,2,3,4,5
  """

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=_DESCRIPTION,
        criteria_spans_query_template=_REASON_QUERY,
        state_code=StateCode.US_PA,
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        sessions_dataset=SESSIONS_DATASET,
        meets_criteria_default=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
