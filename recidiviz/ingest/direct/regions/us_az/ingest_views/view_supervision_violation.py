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
"""Query containing information about warrants issued by community supervision."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This cleans raw data from the AZ_DOC_HWD_WARRANT raw data, which includes information
-- about all warrants that have been issued as a result of supervision violations.
-- All of these are intended to lead to a revocation.
warrants AS (
SELECT DISTINCT 
  WARRANT_ID AS violation_external_id, -- violation response external ID
  PERSON_ID AS person_external_id, -- state person external ID
  AGENT_ID AS reporting_staff, -- violation response metadata staff_issuing_warrant
  WARRANT_DTM AS violation_date, -- violation date
  COALESCE(AMENDMENT_DTM,ISSUED_DTM,WARRANT_DTM) AS response_date, -- response date
  type.DESCRIPTION AS violation_type_description, -- violation type
  misdemeanor_flag AS violation_type_misdemeanor, -- violation type
  felony_flag AS violation_type_felony, -- violation type
  COMPLETED_FLAG AS response_is_completed, -- response.is_draft ? 
  reason.DESCRIPTION AS violation_result, -- response decision
  status.DESCRIPTION AS violation_result_status, -- response decision
  CAST(NULL AS STRING) AS sanction_type, 
  CAST(NULL AS STRING) AS intervention_type,
  'WARRANT' AS source, 
FROM
  {AZ_DOC_HWD_WARRANT} warrant
LEFT JOIN
  {LOOKUPS} status
ON
  (STATUS_ID = status.LOOKUP_ID)
LEFT JOIN
  {LOOKUPS} type
ON
  (TYPE_ID = type.LOOKUP_ID)
LEFT JOIN
  {LOOKUPS} class
ON
  (CLASS_ID = class.LOOKUP_ID)
LEFT JOIN
  {LOOKUPS} reason
ON
  (reason_id = reason.LOOKUP_ID)
WHERE PERSON_ID IS NOT NULL
),
-- This cleans raw data from the AZ_CS_OMS_INT_SANC raw data, which contains information
-- about all sanctions and interventions that result from a supervision violation. This
-- is a distinct set of violations from those that result in warrants being issued, and 
-- do not lead to revocations.
sanctions AS (
SELECT DISTINCT
  INTERVENTION_SANCTION_ID AS violation_external_id, -- violation response external ID
  PERSON_ID AS person_external_id, -- state person external ID
  STAFF_ID AS reporting_staff, -- violation response metadata staff_issuing_warrant
  INTSANC_DATE AS violation_date, -- response date
  INTSANC_DATE AS response_date, -- response date
 -- The sanction is either in response to a VIOLATION or an INT_VIOLATION.
  COALESCE(NULLIF(VIOLATION,'NULL'), NULLIF(INT_VIOLATION,'NULL')) AS violation_type_description,
  CAST(NULL AS STRING) AS violation_type_misdemeanor,
  CAST(NULL AS STRING) AS violation_type_felony,
  'Y' AS response_is_completed,
  CAST(NULL AS STRING) AS violation_result, 
  CAST(NULL AS STRING) AS violation_result_status, 
  CONCAT(sanc_type.DESCRIPTION, '|', IFNULL(OTHER_SANCTION, 'None')) AS sanction_type, -- response decision
  int_type.DESCRIPTION AS intervention_type, -- response decision
  'INT_SANC' AS source
FROM
  {AZ_CS_OMS_INT_SANC} sanction
LEFT JOIN
  {LOOKUPS} status
ON
  (INT_STATUS_ID = status.LOOKUP_ID)
LEFT JOIN
  {LOOKUPS} sanc_type
ON
  (SANCTION_TYPE_ID = sanc_type.LOOKUP_ID)
LEFT JOIN
  {LOOKUPS} condition
ON
  (CONDITION_ID = condition.LOOKUP_ID)
LEFT JOIN
  {LOOKUPS} int_type
ON
  (INTSANC_TYPE_ID = int_type.LOOKUP_ID)
JOIN
  {DPP_EPISODE} dpp_ep
USING
  (DPP_ID)

)

SELECT * FROM warrants
UNION ALL 
SELECT * FROM sanctions
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="supervision_violation",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
