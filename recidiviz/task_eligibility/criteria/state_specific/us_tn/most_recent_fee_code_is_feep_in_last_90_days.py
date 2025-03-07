# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""Spans of time during which a client in TN has a FEEP (fees or exemptions are current) contact code
in the most recent 3 months and that is their most recent FEE code
"""

from google.cloud import bigquery

from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_CRITERIA_NAME = "US_TN_MOST_RECENT_FEE_CODE_IS_FEEP_IN_LAST_90_DAYS"

_QUERY_TEMPLATE = f"""
    WITH fee_codes_spans AS (
        SELECT
          state_code,
          person_id,
          contact_date AS start_date,
          contact_type,
          contact_date AS latest_fee_contact_date,
          CASE WHEN contact_type = 'FEEP' THEN DATE_ADD(contact_date, INTERVAL 90 DAY) END AS feep_end_date,
          LEAD(contact_date) OVER(PARTITION BY person_id ORDER BY contact_date ASC) AS next_fee_contact_date
      FROM (
          SELECT
            pei.state_code,
            pei.person_id,
            CAST(CAST(contact.ContactNoteDateTime AS datetime) AS DATE) AS contact_date,
            contact.ContactNoteType AS contact_type,
        FROM
            `{{project_id}}.us_tn_raw_data_up_to_date_views.ContactNoteType_latest` contact
        INNER JOIN 
            `{{project_id}}.normalized_state.state_person_external_id` pei
        ON
            contact.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        AND 
            pei.id_type = 'US_TN_DOC'
        -- Includes FEEP (FEES OR EXEMPTIONS CURRENT) and FEER (FEE ARREARAGE,  SEE COMMENTS FOR REMEDIAL ACTION)
        WHERE
            contact.ContactNoteType LIKE 'FEE%'    
        -- There's a very small number of duplicates so we deterministically choose FEER over FEEP to be more conservative
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, CAST(CAST(contact.ContactNoteDateTime AS datetime) AS DATE)
                                  ORDER BY CASE WHEN ContactNoteType = 'FEER' THEN 0 ELSE 1 END) = 1
      )
    ), 
    determine_ends AS (
      SELECT
        state_code,
        person_id,
        start_date,
        -- The spans end when there's a) another fee related contact (FEEP or FEER) or b) when the 90-day span of a
        -- FEEP note ends, whichever comes first
        LEAST(
          {nonnull_end_date_clause("feep_end_date")},
          {nonnull_end_date_clause("next_fee_contact_date")}
        ) AS end_date,
        latest_fee_contact_date,
        contact_type,
        contact_type = 'FEEP' AS meets_criteria
      FROM fee_codes_spans
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        meets_criteria,
        TO_JSON(STRUCT(
            latest_fee_contact_date,
            contact_type AS latest_contact_type
        )) AS reason,
        contact_type,
        latest_fee_contact_date,
    FROM determine_ends

"""

VIEW_BUILDER: StateSpecificTaskCriteriaBigQueryViewBuilder = (
    StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        description=__doc__,
        state_code=StateCode.US_TN,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        reasons_fields=[
            ReasonsField(
                name="latest_fee_contact_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date on which latest FEE related note was recorded",
            ),
            ReasonsField(
                name="contact_type",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Latest FEE related note",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
