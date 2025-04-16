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
"""Collect relevant metadata to support Suspension of Direct Supervision in TN."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.workflows.firestore.opportunity_record_query_fragments import (
    array_agg_case_notes_by_external_id,
    join_current_task_eligibility_spans_with_external_id,
)
from recidiviz.calculator.query.state.views.workflows.us_tn.shared_ctes import (
    keep_contact_codes,
    us_tn_get_offense_information,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.dataset_config import (
    task_eligibility_spans_state_specific_dataset,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_NAME = (
    "us_tn_suspension_of_direct_supervision_record"
)

# TODO(#38270): Finish adding necessary information to opportunity record in accordance
# with feedback from TTs.
# TODO(#38270): Use ingested data for `form_information_supervision_office_location`
# (from `location_metadata`, ideally).
US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_QUERY_TEMPLATE = f"""
    WITH base AS (
        {join_current_task_eligibility_spans_with_external_id(
            state_code="'US_TN'",
            tes_task_query_view="suspension_of_direct_supervision_materialized",
            id_type="'US_TN_DOC'",
            eligible_and_almost_eligible_only=True,
            additional_columns="tes.reasons_v2",
        )}
    ),
    current_sentences AS (
        SELECT 
            'US_TN' AS state_code,
            person_id,
            conviction_counties,
            current_offenses,
            sentence_start_date AS sentence_start_date_earliest,
            expiration_date AS sentence_end_date_latest,
            serving_life_sentence,
        FROM ({us_tn_get_offense_information(in_projected_completion_array=True)})
    ),
    /* A single `ContactNoteComment` can be associated with multiple `ContactNoteType`
    values, and there can be multiple contact notes entered per day. Below, we pull
    relevant contact notes by type, aggregating them at the date level. This may be
    slightly noisier than would be ideal, but it may help us capture more complete
    information (e.g., if there are two related notes in one day, with the second
    following up on the first). */ 
    relevant_codes AS (
        SELECT
            person_id,
            contact_date,
            contact_type,
        FROM `{{project_id}}.{{analyst_views_dataset}}.us_tn_relevant_contact_codes_materialized`
    ),
    comments_clean AS (
        SELECT
            person_id,
            contact_date,
            contact_comment,
        FROM `{{project_id}}.{{analyst_views_dataset}}.us_tn_contact_comments_preprocessed_materialized`
    ),
    contact_latest_negative_arrest_check AS (
        -- latest negative arrest check
        {keep_contact_codes(
            codes_cte="relevant_codes",
            comments_cte="comments_clean",
            where_clause_codes_cte="WHERE contact_type='ARRN'",
            output_name="latest_negative_arrest_check",
            keep_last=True,
        )}
    ),
    case_notes AS (
        -- get case notes by external ID
        SELECT
            pei.external_id,
            case_notes_by_person_id.* EXCEPT (person_id),
        -- union all contact notes to be displayed in side panel
        FROM (
            -- latest contact note related to NCIC check
            SELECT
                person_id,
                contact_type AS note_title,
                contact_date AS event_date,
                contact_comment AS note_body,
                "LATEST NCIC CHECK" AS criteria,
            FROM (
                {keep_contact_codes(
                    codes_cte="relevant_codes",
                    comments_cte="comments_clean",
                    where_clause_codes_cte="WHERE contact_type IN ('BBNN', 'BBNP')",
                    keep_last=True,
                )}
            )
            UNION ALL
            -- contact notes related to arrests
            SELECT
                person_id,
                contact_type AS note_title,
                contact_date AS event_date,
                contact_comment AS note_body,
                "ARRESTS" AS criteria,
            FROM (
                {keep_contact_codes(
                    codes_cte="relevant_codes",
                    comments_cte="comments_clean",
                    where_clause_codes_cte="WHERE contact_type='ARRP'",
                    keep_last=False,
                )}
            )
            UNION ALL
            -- contact notes related to substance use
            SELECT
                person_id,
                contact_type AS note_title,
                contact_date AS event_date,
                contact_comment AS note_body,
                "SUBSTANCE USE HISTORY" AS criteria,
            FROM (
                {keep_contact_codes(
                    codes_cte="relevant_codes",
                    comments_cte="comments_clean",
                    where_clause_codes_cte="WHERE contact_type='DRUP' OR contact_type LIKE 'FSW%'",
                    keep_last=False,
                )}
            )
            UNION ALL
            -- contact notes related to negative drug tests
            SELECT
                person_id,
                contact_type AS note_title,
                contact_date AS event_date,
                contact_comment AS note_body,
                "SUBSTANCE USE HISTORY - NEGATIVE SCREENS" AS criteria,
            FROM (
                {keep_contact_codes(
                    codes_cte="relevant_codes",
                    comments_cte="comments_clean",
                    where_clause_codes_cte="WHERE contact_type IN ('DRUN', 'DRUM', 'DRUX')",
                    keep_last=False,
                )}
            )
            UNION ALL
            -- latest contact note related to special conditions
            SELECT
                person_id,
                contact_type AS note_title,
                contact_date AS event_date,
                contact_comment AS note_body,
                "LATEST SPECIAL CONDITIONS" AS criteria,
            FROM (
                {keep_contact_codes(
                    codes_cte="relevant_codes",
                    comments_cte="comments_clean",
                    where_clause_codes_cte="WHERE contact_type IN ('SPEC', 'SPET', 'XSPE')",
                    keep_last=True,
                )}
            )
            UNION ALL
            -- latest contact note related to employment
            SELECT
                person_id,
                contact_type AS note_title,
                contact_date AS event_date,
                contact_comment AS note_body,
                "LATEST EMPLOYMENT" AS criteria,
            FROM (
                {keep_contact_codes(
                    codes_cte="relevant_codes",
                    comments_cte="comments_clean",
                    where_clause_codes_cte="WHERE contact_type LIKE '%EMP%'",
                    keep_last=True,
                )}
            )
        ) case_notes_by_person_id
        LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
            ON case_notes_by_person_id.person_id=pei.person_id
            AND pei.state_code='US_TN'
            AND pei.id_type='US_TN_DOC'
    ),
    case_notes_aggregated AS (
        {array_agg_case_notes_by_external_id(
            from_cte="base",
            left_join_cte="case_notes",
        )}
    )
    SELECT
        base.state_code,
        base.external_id,
        base.reasons_v2 AS reasons,
        base.ineligible_criteria,
        base.is_eligible,
        base.is_almost_eligible,
        -- metadata
        contact_latest_negative_arrest_check.latest_negative_arrest_check AS metadata_latest_negative_arrest_check,
        -- case notes
        cna.case_notes,
        -- form information
        current_sentences.conviction_counties AS form_information_conviction_counties,
        ARRAY_TO_STRING(
            /* The SDS form requests the "most serious" charge. Although we're not
            identifying that specific charge here, we'll provide a list of distinct
            offenses for the officer to filter as needed.
            Here, we reduce the array of all offenses to just an array of distinct
            offenses (so that it's easier for the officer to sort through). */
            ARRAY(SELECT DISTINCT offense FROM UNNEST(current_sentences.current_offenses) AS offense ORDER BY offense),
            '; '
        ) AS form_information_conviction_charge,
        current_sentences.sentence_start_date_earliest AS form_information_sentence_date,
        IF(
            current_sentences.serving_life_sentence,
            'Life',
            CAST(DATE_DIFF(current_sentences.sentence_end_date_latest, pss.start_date, YEAR) AS STRING)
        ) AS form_information_supervision_duration,
        site.AddressCity AS form_information_supervision_office_location,
    FROM base
    LEFT JOIN contact_latest_negative_arrest_check
        ON base.person_id=contact_latest_negative_arrest_check.person_id
    LEFT JOIN case_notes_aggregated cna
        ON base.external_id=cna.external_id
    LEFT JOIN current_sentences
        ON base.state_code=current_sentences.state_code
        AND base.person_id=current_sentences.person_id
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.prioritized_supervision_sessions_materialized` pss
        ON base.state_code=pss.state_code
        AND base.person_id=pss.person_id
        AND CURRENT_DATE('US/Eastern') BETWEEN pss.start_date AND {nonnull_end_date_exclusive_clause('pss.end_date_exclusive')}
    LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_sub_sessions_materialized` css
        ON base.state_code=css.state_code
        AND base.person_id=css.person_id
        AND CURRENT_DATE('US/Eastern') BETWEEN css.start_date AND {nonnull_end_date_exclusive_clause('css.end_date_exclusive')}
    LEFT JOIN `{{project_id}}.{{us_tn_raw_data_up_to_date_dataset}}.Site_latest` site
        ON css.supervision_office=site.SiteID
"""

US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_NAME,
    view_query_template=US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_QUERY_TEMPLATE,
    description=__doc__,
    task_eligibility_dataset=task_eligibility_spans_state_specific_dataset(
        StateCode.US_TN,
    ),
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    analyst_views_dataset=dataset_config.ANALYST_VIEWS_DATASET,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.build_and_print()
