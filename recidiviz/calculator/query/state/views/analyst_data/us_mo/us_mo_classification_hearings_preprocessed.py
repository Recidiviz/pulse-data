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
"""Query for classification hearings in Missouri prisons
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_VIEW_NAME = (
    "us_mo_classification_hearings_preprocessed"
)

US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_DESCRIPTION = """
    Query for classification hearings in Missouri prisons
    """

US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_QUERY_TEMPLATE = """
    WITH hearings_dedup_type AS (
        SELECT DISTINCT
            p.state_code, 
            p.person_id, 
            FIRST_VALUE(h.JU_CSQ) OVER same_hearing_dates AS hearing_id,
            -- JU_BA: Classification Hearing Date
            SAFE.PARSE_DATE("%Y%m%d", h.JU_BA) AS hearing_date,
            -- JU_AY: Classification Hearing Next Review Date ("0" is null, handled by SAFE.PARSE_DATE)
            SAFE.PARSE_DATE("%Y%m%d", FIRST_VALUE(h.JU_AY) OVER same_hearing_dates) AS next_review_date,
            h.JU_HRT AS hearing_type,
            h.JU_PLN AS hearing_facility,
        FROM `{project_id}.{us_mo_raw_data_up_to_date_dataset}.LBAKRDTA_TAK293_latest` h
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` p
        ON
            h.JU_DOC = p.external_id
            AND p.state_code = "US_MO"
        WHERE
            -- Subset to Classification Hearings
            h.JU_FOR = "CLA"
        -- Dedup hearings on same day of same type
        WINDOW same_hearing_dates AS (
            PARTITION BY p.person_id, h.JU_BA, h.JU_HRT
            ORDER BY CAST(h.JU_SEQ AS INT64) DESC
        )
    )
    ,
    -- Dedup hearings on the same date. NOTE: Multiple hearings of different types on the same date are likely valid and
    -- intended in some cases, and without this dedup would result in ~150 cases of overlapping spans. This 
    -- deduplication bypasses the issue of multiple "threads" of hearings for the same person by assuming some hearing
    -- types have priority over others, which will have their "next review date" ignored.
    -- TODO(#18536): Verify this approach with MODOC staff
    hearings AS (
        SELECT DISTINCT
            state_code,
            person_id,
            hearing_date,
            FIRST_VALUE(hearing_id) OVER w as hearing_id,
            FIRST_VALUE(next_review_date) OVER w AS next_review_date,
            FIRST_VALUE(hearing_type) OVER w AS hearing_type,
            FIRST_VALUE(hearing_facility) OVER w AS hearing_facility,
        FROM hearings_dedup_type
        
        -- TODO(#18774) Determine priority order for hearing types
        WINDOW w AS (
            PARTITION by person_id, state_code, hearing_date
            ORDER BY CASE hearing_type
                WHEN "ADS30" THEN 1
                WHEN "ADS60" THEN 2
                WHEN "ADS90" THEN 3
                WHEN "TASC" THEN 4
                WHEN "INITIA" THEN 5
                WHEN "PC" THEN 6
                WHEN "OTHER" THEN 7
                ELSE 8 END
        )
    )
    ,
    hearing_comments AS (
        SELECT
            ISCSQ_ as hearing_id,
            -- Trim whitespace and quotation marks to concatenate comments across rows.
            -- Comment lines in raw table are sometimes separated in reality by line breaks, and sometimes
            -- by a single space. This inserts line breaks as a general case that doesn't look terrible.
            STRING_AGG(TRIM(ISCMNT, " \\""), "\\n" ORDER BY CAST(ISCLN_ AS INT64) ASC) AS hearing_comments,
        FROM `{project_id}.{us_mo_raw_data_up_to_date_dataset}.LBAKRDTA_TAK294_latest`
        GROUP BY 1
    )
    SELECT
        state_code,
        person_id,
        hearing_id,
        hearings.hearing_date,
        hearings.hearing_facility,
        hearings.hearing_type,
        hearing_comments.hearing_comments AS hearing_comments,
        hearings.next_review_date AS next_review_date,
    FROM hearings
    LEFT JOIN hearing_comments
    USING(hearing_id)
    WHERE hearings.hearing_date IS NOT NULL
"""

US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_VIEW_NAME,
    view_query_template=US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_QUERY_TEMPLATE,
    description=US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_VIEW_BUILDER.build_and_print()
