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
"""US_ME - SCCP Eligibility"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_DAY_0_SCCP_VIEW_NAME = "us_me_day_0_sccp"

US_ME_DAY_0_SCCP_VIEW_DESCRIPTION = """SCCP Eligibility Criteria for Maine"""

US_ME_DAY_0_SCCP_QUERY_TEMPLATE = """
WITH current_incarcerated_population AS (
  SELECT
    cs.person_id,
    external_id AS person_external_id,
    TRIM(CONCAT(
      JSON_EXTRACT_SCALAR(full_name, "$.given_names"),
      " ",
      JSON_EXTRACT_SCALAR(full_name, "$.surname")
    )) AS person_name,
    compartment_location_end AS current_location,
    ss.start_date AS incarceration_start_date,
  FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` cs
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id`
    USING (state_code, person_id)
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person`
    USING (state_code, person_id)
  LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` ss
    ON cs.person_id = ss.person_id
    AND cs.session_id BETWEEN ss.session_id_start AND ss.session_id_end
  WHERE cs.state_code = "US_ME"
    AND cs.end_date IS NULL
    AND cs.compartment_level_1 = "INCARCERATION"
),
current_sentences AS (
  SELECT
    person_id,
    MAX(projected_min_release_date) AS projected_release_date,
  FROM `{project_id}.{normalized_state_dataset}.state_incarceration_sentence`
  WHERE state_code = "US_ME"
    -- sentences that have not been completed
    AND completion_date IS NULL
    AND status != "COMPLETED"
  GROUP BY 1
),
sentence_metrics AS (
  SELECT
    person_id,
    projected_release_date,
    ROUND(DATE_DIFF(projected_release_date, incarceration_start_date, DAY) / 365.25, 0) <= 5 AS lte_5_years,
    ROUND(DATE_DIFF(projected_release_date, CURRENT_DATE('US/Eastern'), DAY) / 30, 0) <= 30 AS lte_30_months_remaining,
    DATE_DIFF(CURRENT_DATE('US/Eastern'), incarceration_start_date, DAY) / DATE_DIFF(projected_release_date, incarceration_start_date, DAY) >= 1/2 AS gte_half_sentence_served,
    DATE_DIFF(CURRENT_DATE('US/Eastern'), incarceration_start_date, DAY) / DATE_DIFF(projected_release_date, incarceration_start_date, DAY) >= 2/3 AS gte_two_thirds_sentence_served,
  FROM current_incarcerated_population
  INNER JOIN current_sentences
    USING (person_id)
),
custody_level AS (
  SELECT
    pei.person_id,
    -- cl.CIS_100_CLIENT_ID AS person_external_id,
    CAST(CAST(LEFT(cl.CUSTODY_DATE, 19) AS DATETIME) AS DATE) AS custody_level_date,
    -- cl.CIS_1017_CLIENT_SYS_CD,
    UPPER(cs.CLIENT_SYS_DESC) AS custody_level,
  FROM `{project_id}.{us_me_raw_data_dataset}.CIS_112_CUSTODY_LEVEL` cl
  INNER JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_1017_CLIENT_SYS_latest` cs
    ON cl.CIS_1017_CLIENT_SYS_CD = cs.CLIENT_SYS_CD
  INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
    ON cl.CIS_100_CLIENT_ID = pei.external_id
    AND pei.state_code = "US_ME"
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY CAST(LEFT(cl.CUSTODY_DATE, 19) AS DATETIME) DESC
  ) = 1
  ORDER BY
    pei.person_id,
    CAST(LEFT(cl.CUSTODY_DATE, 19) AS DATETIME)
)

SELECT
  person_external_id,
  person_name,
  current_location,
  incarceration_start_date,
  projected_release_date,
  custody_level,
FROM sentence_metrics
INNER JOIN current_incarcerated_population
  USING (person_id)
LEFT JOIN custody_level cl
  USING (person_id)
WHERE
  -- less than/equal to 30 months remaining on sentence
  lte_30_months_remaining
  -- if less than/equal to 5 year sentence, half sentence served, otherwise 2/3 sentence served
  AND IF(lte_5_years, gte_half_sentence_served, gte_two_thirds_sentence_served)
  -- projected release date is >= 90 days from now (about how long it takes to process the SCCP transfer)
  AND projected_release_date >= DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 90 DAY)
  AND cl.custody_level IN (
    "MINIMUM",
    "COMMUNITY"
  )
ORDER BY incarceration_start_date, projected_release_date, person_external_id
"""

US_ME_DAY_0_SCCP_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=US_ME_DAY_0_SCCP_VIEW_NAME,
    dataset_id=ANALYST_VIEWS_DATASET,
    description=US_ME_DAY_0_SCCP_VIEW_DESCRIPTION,
    view_query_template=US_ME_DAY_0_SCCP_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=False,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_me"),
    us_me_raw_data_dataset=raw_tables_dataset_for_region("us_me"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_DAY_0_SCCP_VIEW_BUILDER.build_and_print()
