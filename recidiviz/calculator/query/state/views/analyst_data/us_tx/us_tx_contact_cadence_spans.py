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
# =============================================================================
"""View that represents spans of time over which a client in TX has a given
set of contact standards."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "us_tx_contact_cadence_spans"

_QUERY_TEMPLATE = f"""WITH
-- Create periods of case type and supervision level information
person_info AS (
   SELECT 
      sp.person_id,
      start_date,
      termination_date AS end_date,
      supervision_level,
      case_type,
      case_type_raw_text,
      sp.state_code,
    FROM `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` sp
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_supervision_case_type_entry` ct
      USING(supervision_period_id)
    WHERE sp.state_code = "US_TX"
        AND supervision_level IS DISTINCT FROM 'IN_CUSTODY'
),
-- Aggregate above periods by supervision_level and case_type
person_info_agg AS (
    {aggregate_adjacent_spans(
        table_name='person_info',
        index_columns=["person_id", "state_code"],
        attribute=['supervision_level', 'case_type', 'case_type_raw_text'],
        session_id_output_name='person_info_agg',
        end_date_field_name='end_date'
    )}
),
-- Create a table where each contact is associated with the appropriate cadence and we 
-- begin to count the months the client has been on supervision and the number of 
-- periods they'll have given the contact frequency
contacts_compliance AS (
        SELECT DISTINCT
        p.person_id,
        p.state_code,
        p.start_date,
        p.end_date,
        ca.contact_type,
        p.supervision_level,
        DATE_TRUNC(start_date, MONTH) AS month_start,
        p.case_type, 
        CAST(ca.frequency_in_months AS INT64) AS frequency_in_months,
        CAST(ca.quantity AS INT64) AS quantity,
        DATE_DIFF(COALESCE(end_date, CURRENT_DATE), start_date, MONTH) + 1 AS total_months,
        FLOOR ((DATE_DIFF(COALESCE(end_date, CURRENT_DATE), start_date, MONTH) + 1)/CAST(ca.frequency_in_months AS INT64)) as num_periods 
    FROM person_info_agg p
    LEFT JOIN `{{project_id}}.{{raw_data_up_to_date_dataset}}.RECIDIVIZ_REFERENCE_ContactCadence_latest` ca
        ON p.case_type = ca.case_type
        AND p.supervision_level = ca.supervision_level
    -- Remove rows with no contact requirements
    WHERE ca.contact_type IS NOT NULL
),
-- There are times where a client has two supervision levels or case types within 
-- a single month. In order to not have multiple contact cadences, we choose the contact
-- cadence that is associated with the most recent supervision level / case type in a 
-- month. 
single_contacts_compliance AS (
    SELECT
        *
    FROM contacts_compliance
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, month_start, contact_type ORDER BY start_date DESC) = 1
),
-- Creates sets of empty periods that are the duration of the contact frequency
empty_periods AS (
    SELECT 
        person_id,
        state_code,
        contact_type,
        supervision_level,
        case_type,
        quantity,
        start_date,
        end_date,
        frequency_in_months,
        -- For each span, calculate the starting month and ending month
        month_start + INTERVAL x * frequency_in_months MONTH AS month_start,
        -- Calculate the end of the span (last day of the month)
        LAST_DAY(month_start + INTERVAL (x + 1) * frequency_in_months - 1 MONTH) AS month_end
    FROM single_contacts_compliance,
    UNNEST(GENERATE_ARRAY(0, CAST(num_periods AS INT64))) AS x
),
-- There are times where periods are created in advance for a former contact cadence
-- we want to remove these periods so that only periods with the appropriate cadence
-- are kept in a given time. 
clean_empty_periods AS 
(
    SELECT
        *
    FROM empty_periods
    WHERE end_date IS NULL OR month_end < end_date
)
SELECT * FROM clean_empty_periods
"""

US_TX_CONTACT_CADENCE_SPANS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=__doc__,
    view_query_template=_QUERY_TEMPLATE,
    raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TX, instance=DirectIngestInstance.PRIMARY
    ),
    normalized_state_dataset="us_tx_normalized_state",
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TX_CONTACT_CADENCE_SPANS_VIEW_BUILDER.build_and_print()
