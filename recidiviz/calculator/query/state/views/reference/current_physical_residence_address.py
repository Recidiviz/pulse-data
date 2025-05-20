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
"""View containing someone's current physical residence address"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# States should only be added to this list if we already support downstream products
# that read from `state_person.current_address`. Otherwise, states should hydrate
# state_person_address_periods to get current address info to flow into the
# current_physical_residence_address view.
# TODO(#42457): When this set is empty, delete the
#  legacy_state_person_based_current_addresses CTE below
_STATES_WITH_NO_STRUCTURED_ADDRESS_INFO_INGESTED = {
    # TODO(#42458): Hydrate address periods for AZ
    StateCode.US_AZ,
    # TODO(#42459): Hydrate address periods for IX
    StateCode.US_IX,
    # TODO(#42460): Hydrate address periods for PA
    StateCode.US_PA,
    # TODO(#42461): Hydrate address periods for TN
    StateCode.US_TN,
    # TODO(#42462): Hydrate address periods for TX (this will require either getting
    #  structured address info from them or building our own address parsing at ingest
    #  time).
    StateCode.US_TX,
}

_NO_STRUCTURED_ADDRESSES_STATES_STR = list_to_query_string(
    sorted(s.value for s in _STATES_WITH_NO_STRUCTURED_ADDRESS_INFO_INGESTED),
    quoted=True,
)

CURRENT_PHYSICAL_RESIDENCE_ADDRESS_VIEW_NAME = "current_physical_residence_address"

CURRENT_PHYSICAL_RESIDENCE_ADDRESS_QUERY_TEMPLATE = f"""
WITH one_line_address_period_based_current_addresses AS (
  SELECT
    person_address_period_id,
    STRING_AGG(address_line, ', ' ORDER BY line_offset) AS full_address_one_line
  FROM
    `{{project_id}}.normalized_state.state_person_address_period`,
    UNNEST(SPLIT(full_address, '\\n')) AS address_line WITH OFFSET AS line_offset
  GROUP BY person_address_period_id
)
,
address_period_based_current_addresses AS (
    SELECT 
        state_code,
        person_id,
        full_address,
        full_address_one_line,
        address_line_1,
        address_line_2,
        address_city,
        address_state,
        address_country,
        address_zip,
        address_county
    FROM `{{project_id}}.normalized_state.state_person_address_period`
    JOIN one_line_address_period_based_current_addresses
    USING (person_address_period_id)
    WHERE 
        state_code NOT IN ({_NO_STRUCTURED_ADDRESSES_STATES_STR})
        AND {today_between_start_date_and_nullable_end_date_exclusive_clause("address_start_date", "address_end_date")}
        AND address_type = 'PHYSICAL_RESIDENCE'
    -- Deduplicate current addresses
    -- TODO(#37679), TODO(#37678): Remove qualify once overlapping periods are disallowed for all states
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, person_id 
        ORDER BY 
            -- Prioritize verified addresses, then later start dates
            CAST(IFNULL(address_is_verified, FALSE) AS INT64) DESC, 
            address_start_date DESC,
            -- Otherwise take the latest entry in `state_person_address_period`
            person_address_period_id DESC
        ) = 1
),
legacy_state_person_based_current_addresses AS (
    SELECT
        state_code,
        person_id,
        current_address AS full_address
    FROM `{{project_id}}.normalized_state.state_person`
    WHERE 
        state_code IN ({_NO_STRUCTURED_ADDRESSES_STATES_STR})
        AND current_address IS NOT NULL
)
SELECT   
  state_code,
  person_id,
  full_address,
  address_line_1,
  address_line_2,
  address_city,
  address_state,
  address_country,
  address_zip,
  address_county,
  TRUE AS has_structured_address_info
FROM address_period_based_current_addresses

UNION ALL

SELECT   
  state_code,
  person_id,
  full_address,
  NULL AS address_line_1,
  NULL AS address_line_2,
  NULL AS address_city,
  NULL AS address_state,
  NULL AS address_country,
  NULL AS address_zip,
  NULL AS address_county,
  FALSE AS has_structured_address_info
FROM legacy_state_person_based_current_addresses
"""

CURRENT_PHYSICAL_RESIDENCE_ADDRESS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=CURRENT_PHYSICAL_RESIDENCE_ADDRESS_VIEW_NAME,
    view_query_template=CURRENT_PHYSICAL_RESIDENCE_ADDRESS_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_PHYSICAL_RESIDENCE_ADDRESS_VIEW_BUILDER.build_and_print()
