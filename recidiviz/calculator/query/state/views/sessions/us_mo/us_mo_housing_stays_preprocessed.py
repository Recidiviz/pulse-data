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
"""Preprocessed view of housing stays in Missouri"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_HOUSING_STAYS_PREPROCESSED_VIEW_NAME = "us_mo_housing_stays_preprocessed"

US_MO_HOUSING_STAYS_PREPROCESSED_VIEW_DESCRIPTION = (
    """Preprocessed view of housing stays in Missouri"""
)

US_MO_HOUSING_STAYS_PREPROCESSED_QUERY_TEMPLATE = """
    /* {description} */

    SELECT
        p.person_id,
        p.state_code,
        PARSE_DATE("%Y%m%d", h.BN_HS) AS start_date,
        IF (
            h.BN_HE = "0", 
            NULL,
            SAFE.PARSE_DATE("%Y%m%d", h.BN_HE)
        ) AS end_date,
        IFNULL(h.BN_PLN, "EXTERNAL_UNKNOWN") AS facility_code,
        IF (
            h.BN_HPT IS NULL,
            "EXTERNAL_UNKNOWN",
            (
                CASE h.BN_HPT
                WHEN "P" THEN "PERMANENT"
                WHEN "T" THEN "TEMPORARY"
                ELSE
                "INTERNAL_UNKNOWN"
                END
            ) 
        ) AS stay_type,
        h.BN_HPT AS stay_type_raw_text,
        IF (
            h.BN_LRU IS NULL,
            "EXTERNAL_UNKNOWN",
            (
                CASE h.BN_LRU
                WHEN "GNP" THEN "GENERAL_POP"
                WHEN "ADS" THEN "SOLITARY_CONFINEMENT"
                WHEN "TAS" THEN "SOLITARY_CONFINEMENT"
                ELSE "INTERNAL_UNKNOWN"
                END
            ) 
        ) AS confinement_type,
        h.BN_LRU AS confinement_type_raw_text
    FROM {project_id}.{raw_dataset}.LBAKRDTA_TAK017_latest h
    LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id` p
    ON
        h.BN_DOC = p.external_id
        AND p.state_code = "US_MO"
"""

US_MO_HOUSING_STAYS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_MO_HOUSING_STAYS_PREPROCESSED_VIEW_NAME,
    base_dataset=STATE_BASE_DATASET,
    description=US_MO_HOUSING_STAYS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_MO_HOUSING_STAYS_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=False,
    raw_dataset=raw_latest_views_dataset_for_region("us_mo"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_HOUSING_STAYS_PREPROCESSED_VIEW_BUILDER.build_and_print()
