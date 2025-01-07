# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
Helper SQL queries for Texas
"""

from google.cloud import bigquery

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)


def contact_compliance_builder(
    criteria_name: str, description: str, contact_type: str
) -> StateSpecificTaskCriteriaBigQueryViewBuilder:
    """Returns a state-specific criteria view builder indicating the spans of time when
    a person has on supervision has met the contact compliance cadence for a given
    contact type.

    Args:
        criteria_name (str): The name of the criteria
        description (str): The description of the criteria
        contact_type (int): The type of contact
    """

    criteria_query = f"""
WITH
-- Create periods of case type and supervision level information
person_info AS (
   SELECT 
      person_id,
      start_date,
      end_date,
      correctional_level_start AS supervision_level,
      case_type_start AS case_type,
      "US_TX" as state_code,
    FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` 
    WHERE state_code = "US_TX"
        -- Make sure that we have enough info to create span
        AND correctional_level_start IS NOT NULL 
        AND case_type_start IS NOT NULL
),
-- Aggregate above periods by supervision_level and case_type
person_info_agg AS (
    {aggregate_adjacent_spans(
        table_name='person_info',
        attribute=['supervision_level','case_type'],
        session_id_output_name='person_info_agg',
        end_date_field_name='end_date'
    )}
),
-- Create contacts table by adding scheduled prefix and filtering to a single type
contact_info AS (
    SELECT 
        person_id,
        contact_date,
        CONCAT(
            CASE 
                WHEN contact_reason_raw_text = "REGULAR VISIT" 
                    THEN "SCHEDULED " 
                ELSE "UNSCHEDULED " 
            END 
        || contact_method_raw_text ) AS contact_type,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_contact` 
),
-- Create a table where each contact is associated with the appropriate cadence
contacts_compliance AS (
    SELECT DISTINCT
        p.person_id,
        c.contact_date,
        c.contact_type,
        CAST(ca.frequency_in_months AS INT64) AS frequency_in_months,
        p.supervision_level,
        p.case_type, 
        CAST(ca.quantity AS INT64) AS quantity,
    FROM person_info_agg p
    LEFT JOIN contact_info c
        USING (person_id)
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadence_latest` ca
        ON c.contact_type = ca.contact_type
        AND p.case_type = ca.case_type
    -- Remove rows with no contact requirements
    WHERE ca.contact_type IS NOT NULL 
        AND c.contact_type = "{contact_type}"
        -- We only want to keep the non-optional contacts
        AND ca.replacement IS NULL
),
-- Looks back to see how many contacts were completed in a given time frame
lookback_cte AS
(
    SELECT
        cc.person_id,
        cc.case_type,
        cc.contact_type,
        cc.supervision_level,
        COUNT(*) AS contact_count,
        cc.contact_date,
    FROM contacts_compliance cc
    LEFT JOIN contact_info ci
        ON cc.person_id = ci.person_id
        AND ci.contact_type = cc.contact_type
    WHERE ci.contact_date BETWEEN DATE_SUB(cc.contact_date, INTERVAL cc.frequency_in_months MONTH) AND cc.contact_date
    GROUP BY cc.person_id, cc.case_type, cc.contact_type, cc.contact_date, cc.supervision_level
),
-- Creates periods of time in which a person is compliance given a contact and it's cadence
compliance_periods as (
    SELECT 
        cc.person_id,
        cc.contact_type,
        cc.case_type,
        cc.supervision_level,
        cc.contact_date AS compliant_from,
        DATE_ADD(cc.contact_date, INTERVAL cast(frequency_in_months AS INT64) MONTH) AS compliant_until,
    FROM contacts_compliance cc
    LEFT JOIN lookback_cte USING (person_id,case_type,supervision_level,contact_date)
    WHERE contact_count >= quantity
),
-- Generate compliant periods based on overlapping compliance of the same case type
compliant_segments AS (
  SELECT 
    mt.person_id,
    GREATEST(mt.Start_Date, c.compliant_from) AS start_date,
    CASE 
      WHEN mt.End_Date IS NULL THEN c.compliant_until
      ELSE LEAST(mt.End_Date, c.compliant_until)
    END AS end_date,
    "Compliant" AS Compliance,
    contact_type,
    "US_TX" as state_code,
  FROM person_info mt
  JOIN compliance_periods c
    ON mt.person_id = c.person_id
    AND (c.compliant_from < mt.End_Date OR mt.End_Date is null)
    AND c.compliant_until >= mt.Start_Date
),
-- Create subsessions with attributes of compliance segments
    {create_sub_sessions_with_attributes(table_name='compliant_segments',
        index_columns=['person_id'],
        use_magic_date_end_dates=False,
        end_date_field_name='end_date')}
,
-- Groups sub-sessions
sub_sessions_deduped AS (
    SELECT 
        *
    FROM sub_sessions_with_attributes
        GROUP BY 1,2,3,4,5,6
),
-- Agregates the compliance periods
agg AS ({aggregate_adjacent_spans(
    table_name='sub_sessions_deduped',
    attribute=['contact_type','compliance'],
    session_id_output_name='task_type_eligibility_span_id',
    end_date_field_name='end_date')}
),
-- Creates a table of critical dates and their respective compliance 
event_boundaries AS (
    SELECT person_id, start_date AS event_date, "Non-Compliant" AS compliance
    FROM person_info
    UNION ALL
    SELECT person_id, end_date AS event_date, "Non-Compliant" AS compliance
    FROM person_info
    UNION ALL
    SELECT person_id, start_date AS event_date, "Compliant" AS compliance
    FROM agg
    UNION ALL
    SELECT person_id, end_Date AS event_date, "Non-Compliant" AS compliance
    FROM agg
),
-- Orders critical dates
 ordered_dates AS (
  SELECT 
    person_id,
    event_date,
    compliance,
    LEAD(event_date) OVER (PARTITION BY person_id ORDER BY event_date) AS next_date,
    LAG(event_date) OVER (PARTITION BY person_id ORDER BY event_date) AS prev_date,
  FROM event_boundaries
),
-- Creates final periods of compliance
periods AS (
  SELECT 
    "US_TX" as state_code,
    person_id,
    event_date AS start_date,
    next_date AS end_date,
    CASE WHEN 
        compliance = "Compliant"
            THEN TRUE
        ELSE FALSE
    end as meets_criteria,
    TO_JSON(STRUCT(compliance AS compliance)) AS reason,
    CASE WHEN 
        compliance = "Compliant"
            THEN event_date
        ELSE prev_date
    end as last_contact_date,
    CASE WHEN 
        compliance = "Compliant"
            THEN next_date
        ELSE event_date
    end as contact_due_date,
    "{contact_type}" AS type_of_contact,
  FROM ordered_dates
  WHERE event_date IS NOT NULL
)
SELECT 
  *
FROM periods
"""

    return StateSpecificTaskCriteriaBigQueryViewBuilder(
        criteria_name=criteria_name,
        description=description,
        state_code=StateCode.US_TX,
        criteria_spans_query_template=criteria_query,
        sessions_dataset=SESSIONS_DATASET,
        raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_TX, instance=DirectIngestInstance.PRIMARY
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        reasons_fields=[
            ReasonsField(
                name="last_contact_date",
                type=bigquery.enums.StandardSqlTypeNames.DATE,
                description="Date of the last contact.",
            ),
            ReasonsField(
                name="contact_due_date",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Due date of the contact.",
            ),
            ReasonsField(
                name="type_of_contact",
                type=bigquery.enums.StandardSqlTypeNames.STRING,
                description="Type of contact due.",
            ),
        ],
    )
