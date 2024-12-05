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
""" This view has one record per term and person. It contains detainer related information for residents in
IX. This will be used to create criteria queries for the CRC and XCRC workflows. Does not include historical
spans, just a present view of detainers """

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_DETAINER_SPANS_VIEW_NAME = "us_ix_detainer_spans"

US_IX_DETAINER_SPANS_VIEW_DESCRIPTION = """ This view has one record per term and person. It contains detainer related information for residents in
IX. This will be used to create criteria queries for the CRC and XCRC workflows. Does not include historical
spans, just a present view of detainers """

US_IX_DETAINER_SPANS_QUERY_TEMPLATE = f"""
WITH all_detainers AS (
/* This CTE queries all historical and current detainers, and orders them based on their UpdateDate */
    SELECT
        *, 
        ROW_NUMBER() OVER(PARTITION BY OffenderId,DetainerId ORDER BY UpdateDate) as update_seq_no
      FROM (
        SELECT DISTINCT
          person_id,
          state_code,
          OffenderId, 
          DetainerId,
          DATE(IssuedDate) as IssuedDate, 
          DATE(d.UpdateDate) as UpdateDate,
          CASE WHEN DetainerStatusDesc in ("Closed", "Cancelled", "Deceased") THEN 0 ELSE 1 END AS active_status,
          DetainerTypeId,
          DetainerTypeDesc,
          CriminalChargesDesc,
          Comments,
          AgencyContactName,
          DetainerStatusDesc,
          ds.Abbreviation
        FROM `{{project_id}}.us_ix_raw_data.scl_Detainer` d
        LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_DetainerStatus_latest` ds 
            USING(DetainerStatusId)
        LEFT JOIN `{{project_id}}.{{us_ix_raw_data_up_to_date_dataset}}.scl_DetainerType_latest` dt 
            USING(DetainerTypeId)
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON pei.external_id = OffenderId
            AND pei.state_code = 'US_IX'
            AND pei.id_type = 'US_IX_DOC'
      )
),
detainer_spans AS (
/* This CTE creates spans of time when a detainer had certain attributes */
    SELECT * EXCEPT(update_seq_no, IssuedDate, UpdateDate),
      CASE WHEN update_seq_no = 1 THEN LEAST(IssuedDate, UpdateDate) ELSE UpdateDate END AS start_date,
      LEAD(UpdateDate) OVER(PARTITION BY OffenderId,DetainerId ORDER BY UpdateDate) AS end_date
    FROM all_detainers
)
SELECT *
FROM detainer_spans
--for the final view, only query detainers that are active during that span of time
WHERE active_status = 1 
AND start_date != {nonnull_end_date_clause('end_date')}
"""

US_IX_DETAINER_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_DETAINER_SPANS_VIEW_NAME,
    description=US_IX_DETAINER_SPANS_VIEW_DESCRIPTION,
    view_query_template=US_IX_DETAINER_SPANS_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_DETAINER_SPANS_VIEW_BUILDER.build_and_print()
