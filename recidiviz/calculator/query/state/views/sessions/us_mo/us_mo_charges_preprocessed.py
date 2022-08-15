# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""State-specific preprocessing for MO charge data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_CHARGES_PREPROCESSED_VIEW_NAME = "us_mo_charges_preprocessed"

US_MO_CHARGES_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for MO charge data to add judicial district"""
)

# TODO(#10747): Remove this preprocessing once judicial district code is ingested
US_MO_CHARGES_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH parsed_judicial_district AS (
        SELECT
            "US_MO" AS state_code,
            pei.person_id,
            CONCAT(BS_DOC, "-", BS_CYC, "-", BS_SEO) AS external_id,
            -- Replace the default "999" district value with "EXTERNAL_UNKNOWN"
            IF(BS_CRC = "999", "EXTERNAL_UNKNOWN", BS_CRC) AS judicial_district,
        FROM `{project_id}.{raw_dataset}.LBAKRDTA_TAK022_latest` raw
        INNER JOIN `{project_id}.{state_base_dataset}.state_person_external_id` pei
            ON pei.external_id = raw.BS_DOC
            AND pei.id_type = "US_MO_DOC"
    )
    SELECT
        charge.*,
        judicial_district
    FROM `{project_id}.{analyst_dataset}.state_charge_with_labels_materialized` charge
    LEFT JOIN parsed_judicial_district district
        USING (state_code, person_id, external_id)
    WHERE state_code = "US_MO"
    
    """

US_MO_CHARGES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_MO_CHARGES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_MO_CHARGES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_MO_CHARGES_PREPROCESSED_VIEW_DESCRIPTION,
    raw_dataset=raw_latest_views_dataset_for_region("us_mo"),
    state_base_dataset=STATE_BASE_DATASET,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_CHARGES_PREPROCESSED_VIEW_BUILDER.build_and_print()
