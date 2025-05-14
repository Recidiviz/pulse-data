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
"""View that, for each person_id, selects the appropriate external_id that we should
display for that person in the UI of products of a given system type.

This ID may be a type that changes for a person over time, and will generally be the
"most current" version of that id.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.reference.product_person_external_id_helpers import (
    product_display_external_id_types_query,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string_formatting import fix_indent

PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_NAME = "product_display_person_external_ids"


def _display_ids_for_system_type_query(system_type: StateSystemType) -> str:
    """Builds a query that returns every person's display external id for products
    with the given system type.
    """
    return f"""
SELECT
    state_code,
    person_id,
    external_id AS display_person_external_id,
    id_type AS display_person_external_id_type,
    "{system_type.value}" AS system_type
FROM `{{project_id}}.normalized_state.state_person_external_id`
JOIN (
{fix_indent(product_display_external_id_types_query(system_type), indent_level=4)}
)
USING (state_code, id_type)
WHERE
    -- Ingest pipelines enforce that this is true for exactly one id of a given type
    -- for a given person.
    is_current_display_id_for_type
"""


PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_QUERY_TEMPLATE = f"""
WITH 
# TODO(#41541): CA CDC numbers are not yet ingested, so we are pulling from raw data 
#  instead. Delete this CTE and remove the CA-specific logic below once we've ingested
#  US_CA_CDCNO ids.
us_ca_supervision_display_ids AS (
    SELECT
        "US_CA" AS state_code,
        person_id,
        Cdcno AS display_person_external_id, 
        "US_CA_CDCNO" AS display_person_external_id_type,
        "{StateSystemType.SUPERVISION.value}" AS system_type
    FROM `{{project_id}}.us_ca_raw_data_up_to_date_views.PersonParole_latest`
    JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
    ON OffenderId = pei.external_id AND pei.id_type = "US_CA_DOC"
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id ORDER BY display_person_external_id DESC
    ) = 1
),
# TODO(#41840): Remove this logic when this prioritization logic is encoded in the new
# is_current_display_id_for_type field on person_external_id and that field is 
# referenced in _display_ids_for_system_type_query().
us_ne_display_ids AS (
    WITH ne_display_inmateNumbers AS (
      SELECT curr.inmateNumber
      FROM
        `{{project_id}}.us_ne_raw_data_up_to_date_views.InmatePreviousId_latest` curr
      LEFT JOIN
      `{{project_id}}.us_ne_raw_data_up_to_date_views.InmatePreviousId_latest` next
      ON curr.internalId = next.internalId AND curr.inmateNumber = next.prevInmateNumber
      QUALIFY ROW_NUMBER() OVER (
          PARTITION BY curr.internalId 
          ORDER BY
            -- First prioritize numbers that don't have an inmateNumber that comes after them
            IF(next.internalId IS NULL, 0, 1),
            -- If a person has multiple with no following inmateNumber, choose the most recent one
            DATE(curr.receivedDate) DESC
      ) = 1
    ),
    ne_display_ids AS (
        SELECT
            "US_NE" AS state_code,
            person_id,
            external_id AS display_person_external_id,
            id_type AS display_person_external_id_type
        FROM `{{project_id}}.normalized_state.state_person_external_id`
        WHERE state_code = "US_NE" AND id_type = "US_NE_ID_NBR" AND external_id IN (
          SELECT inmateNumber FROM ne_display_inmateNumbers
        )
    )
    
    SELECT
        state_code,
        person_id,
        display_person_external_id,
        display_person_external_id_type,
        "SUPERVISION" AS system_type
    FROM ne_display_ids

    UNION ALL
    
    SELECT
        state_code,
        person_id,
        display_person_external_id,
        display_person_external_id_type,
        "INCARCERATION" AS system_type
    FROM ne_display_ids
),
incarceration_display_ids AS (
{fix_indent(_display_ids_for_system_type_query(StateSystemType.INCARCERATION), indent_level=4)}
),
supervision_display_ids AS (
{fix_indent(_display_ids_for_system_type_query(StateSystemType.SUPERVISION), indent_level=4)}
)

SELECT
    state_code,
    person_id,
    display_person_external_id,
    display_person_external_id_type,
    system_type
FROM incarceration_display_ids
WHERE state_code NOT IN (
    # TODO(#41840): Delete this filter once we've translated NE-specific prioritization
    # logic to live on the state_person_external_id
    "US_NE"
)

UNION ALL

# TODO(#41541): Delete this clause once we've ingested US_CA_CDCNO ids.
SELECT 
    state_code,
    person_id,
    display_person_external_id,
    display_person_external_id_type,
    system_type
FROM us_ca_supervision_display_ids

UNION ALL

# TODO(#41840): Delete this clause once we've translated NE-specific prioritization
# logic to live on the state_person_external_id
SELECT 
    state_code,
    person_id,
    display_person_external_id,
    display_person_external_id_type,
    system_type
FROM us_ne_display_ids

UNION ALL

SELECT
    state_code,
    person_id,
    display_person_external_id,
    display_person_external_id_type,
    system_type
FROM supervision_display_ids
WHERE state_code NOT IN (
    # TODO(#41541): Delete this filter once we've ingested US_CA_CDCNO ids.
    "US_CA",
    # TODO(#41840): Delete this filter once we've translated NE-specific prioritization
    # logic to live on the state_person_external_id
    "US_NE"
)
"""


PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_NAME,
    view_query_template=PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRODUCT_DISPLAY_PERSON_EXTERNAL_IDS_VIEW_BUILDER.build_and_print()
