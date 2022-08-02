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
"""US_ME - Supervision Downgrade Eligibility"""

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

US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_NAME = "us_me_day_0_supervision_downgrade"

US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_DESCRIPTION = (
    """Supervision Downgrade Eligibility Criteria for Maine"""
)

US_ME_DAY_0_SUPERVISION_DOWNGRADE_QUERY_TEMPLATE = """
-- Current probation population
WITH current_probation_population AS (
  SELECT
    cs.person_id,
    external_id AS person_external_id,
    TRIM(CONCAT(
      JSON_EXTRACT_SCALAR(full_name, "$.given_names"),
      " ",
      JSON_EXTRACT_SCALAR(full_name, "$.surname")
    )) AS person_name,
    p.birthdate,
    cs.start_date,
  FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` cs
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id`
    USING (state_code, person_id)
  LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person` p
    USING (state_code, person_id)
  WHERE cs.state_code = "US_ME"
    AND cs.end_date IS NULL
    AND cs.compartment_level_2 = "PROBATION"
),
latest_assessment AS (
  SELECT
    person_id,
    assessment_date,
    assessment_level,
    assessment_level_raw_text,
    CASE
      WHEN assessment_level_raw_text = "ADMINISTRATIVE" THEN 0
      WHEN assessment_level_raw_text = "LOW" THEN 1
      WHEN assessment_level_raw_text = "MODERATE" THEN 3
      WHEN assessment_level_raw_text = "HIGH" THEN 3
      WHEN assessment_level_raw_text = "MAXIMUM" THEN 12
    END AS required_face_to_face,
    CASE
      WHEN assessment_level_raw_text = "ADMINISTRATIVE" THEN 0
      WHEN assessment_level_raw_text = "LOW" THEN 0
      WHEN assessment_level_raw_text = "MODERATE" THEN 0
      WHEN assessment_level_raw_text = "HIGH" THEN 1
      WHEN assessment_level_raw_text = "MAXIMUM" THEN 3
    END AS required_home_visit,
  FROM `{project_id}.{normalized_state_dataset}.state_assessment`
  WHERE state_code = "US_ME"
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY assessment_date DESC
  ) = 1
),
CIS_204_GEN_NOTE_latest AS (
  SELECT * EXCEPT (file_id, update_datetime)
  FROM `{project_id}.{us_me_raw_data_dataset}.CIS_204_GEN_NOTE`
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY Note_Id
    ORDER BY update_datetime DESC
  ) = 1
),
# TODO(#14199) replace with ingested data once available
contacts AS (
  SELECT
    Note_Id,
    CAST(CAST(LEFT(Note_Date, 19) AS DATETIME) AS DATE) AS contact_date,
    Cis_100_Client_Id AS person_external_id,
    lt.E_Ccs_Location_Type_Desc AS location_type,
    l.Location_Name AS location_name,
    nt.E_Note_Type_Desc AS note_type,
    cm.E_Mode_Desc AS contact_mode,
    CASE WHEN cm.E_Mode_Desc IN ("Home Visit", "Office Visit") THEN 1 ELSE 0 END AS contact_type_face_to_face,
    CASE WHEN cm.E_Mode_Desc IN ("Home Visit") THEN 1 ELSE 0 END AS contact_type_home_visit,
  FROM CIS_204_GEN_NOTE_latest c
  LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_908_CCS_LOCATION_latest` l
    ON c.Cis_908_Ccs_Location_Id = l.Ccs_Location_Id
  LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_9080_CCS_LOCATION_TYPE_latest` lt
    ON l.Cis_9080_Ccs_Location_Type_Cd = lt.Ccs_Location_Type_Cd
  LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_2041_NOTE_TYPE_latest` nt
    ON c.Cis_2041_Note_Type_Cd = nt.Note_Type_Cd
  LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_2040_CONTACT_MODE_latest` cm
    ON c.Cis_2040_Contact_Mode_Cd = cm.Contact_Mode_Cd
  WHERE Cis_2042_Person_Contacted_Cd = "1"
    AND cm.E_Mode_Desc IN (
      "Home Visit",
      "Office Visit"
    )
),
contact_counts AS (
  SELECT
    person_external_id,
    SUM(contact_type_face_to_face) AS previous_12_weeks_face_to_face,
    SUM(contact_type_home_visit) AS previous_12_weeks_home_visit,
  FROM contacts
  WHERE contact_date >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 12 WEEK)
  GROUP BY 1
)

SELECT *
FROM (
  SELECT
    person_external_id,
    person_name,
    birthdate,
    start_date,
    assessment_date AS latest_assessment_date,
    assessment_level,
    assessment_level_raw_text,
    required_face_to_face,
    required_home_visit,
    COALESCE(previous_12_weeks_face_to_face, 0) AS previous_12_weeks_face_to_face,
    COALESCE(previous_12_weeks_home_visit, 0) AS previous_12_weeks_home_visit,
  FROM current_probation_population p
  INNER JOIN latest_assessment la
    USING (person_id)
  LEFT JOIN contact_counts cc
    USING (person_external_id)
)
WHERE previous_12_weeks_face_to_face > required_face_to_face
  OR previous_12_weeks_home_visit > required_home_visit
"""

US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_NAME,
    dataset_id=ANALYST_VIEWS_DATASET,
    description=US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_DESCRIPTION,
    view_query_template=US_ME_DAY_0_SUPERVISION_DOWNGRADE_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    us_me_raw_data_dataset=raw_tables_dataset_for_region("us_me"),
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_me"),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER.build_and_print()
