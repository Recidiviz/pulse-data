# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Requests for early discharge from supervision sentences in Idaho."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_NAME = "us_id_early_discharge_requests"

US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_DESCRIPTION = (
    """Requests for early discharge from supervision sentences in Idaho."""
)

US_ID_RAW_DATASET = "us_id_raw_data_up_to_date_views"

# These variables are used to infer the status of requests still marked as pending. In recidiviz/recidiviz-research#365
# we observe that ~90% of all requests that are granted are decided within 6 months. We extend the timeframe for parole
# to 12 months because the new (~2019) approval process for parole requests involves 2 hearings which occur quarterly.
PAROLE_ED_MONTHS = "12"
PROBATION_ED_MONTHS = "6"

US_ID_EARLY_DISCHARGE_REQUESTS_QUERY_TEMPLATE = """
    WITH state_ed_requests AS (
        -- This cte contains one row per SENTENCE of an early discharge request.
        SELECT
            state_code,
            person_id,
            -- split ID to match with raw requests table
            SPLIT(external_id, '-')[OFFSET(0)] AS raw_request_id,
            external_id AS raw_request_sentence_id,
            requesting_body_type,
            deciding_body_type_raw_text AS supervision_type,
            -- rename these because we will create inferred columns later
            decision_date AS decision_date_raw,
            decision_status AS decision_status_raw,
            decision AS decision_raw,
            decision_raw_text AS decision_reason_raw,
        FROM `{project_id}.{state_dataset}.state_early_discharge`
        WHERE state_code = 'US_ID'
            -- Invalid decision_status indicates that row has been deleted in raw data
            AND decision_status != 'INVALID'
    ),
    raw_ed_requests AS (
        -- This cte contains one row per early discharge request. It is used to get more granular date information on the request process.
        SELECT
            early_discharge_id AS raw_request_id,
            ofndr_num AS person_external_id,
            CAST(SAFE_CAST(created_by_dt AS DATETIME) AS DATE) AS create_date,
            UPPER(created_by_id) AS supervising_officer_id,
            CAST(SAFE_CAST(sup_aprv_sbmt_dt AS DATETIME) AS DATE) AS supervisor_date,
            CAST(SAFE_CAST(juris_auth_sbmt_dt AS DATETIME) AS DATE) AS submit_date,
        FROM `{project_id}.{us_id_raw_dataset}.early_discharge_latest`
    ),
    raw_ed_requests_sentences AS (
        -- This cte contains one record for each sentence that is included in an early discharge request in raw data.
        SELECT
            -- concat to match id from state table
            CONCAT(early_discharge_id, '-', early_discharge_sent_id) AS raw_request_sentence_id,
            mitt_srl,
            sent_no,
            caseno_seq,
        FROM `{project_id}.{us_id_raw_dataset}.early_discharge_sent_latest`
    ),
    state_ed_requests_sentences AS (
        -- This cte joins state and raw data, and adds a recency rank to indicate multiple requests for discharge
        -- from the same sentence. This will be used to join any identified early discharges to the most recent
        -- request.
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY mitt_srl, sent_no
                ORDER BY create_date DESC
            ) AS request_sentence_recency_rank
        FROM state_ed_requests
        INNER JOIN raw_ed_requests
            USING (raw_request_id)
        INNER JOIN raw_ed_requests_sentences
            USING (raw_request_sentence_id)
    ),
    raw_parole_discharges AS (
        -- This cte identifies all parole sentences that ended via early discharge in raw data.
        SELECT
            mitt_srl,
            sent_no,
            CAST(SAFE_CAST(sent_sat_dtd AS DATETIME) AS DATE) AS sent_sat_dtd,
        FROM `{project_id}.{us_id_raw_dataset}.sentence_latest`
        WHERE sent_disp = 'F' -- Parole Early Discharge
    ),
    raw_probation_discharges AS (
        -- This cte identifies all probation periods that ended via an early discharge in raw data.
        SELECT
            mitt_srl,
            -- coalesce to '0' to match early_discharge_sent table
            COALESCE(caseno_seq, '0') AS caseno_seq,
            CAST(SAFE_CAST(move_dtd AS DATETIME) AS DATE) AS move_dtd,
        FROM `{project_id}.{us_id_raw_dataset}.movement_latest`
        INNER JOIN `{project_id}.{us_id_raw_dataset}.mittimus_latest`
            USING (docno, incrno)
        WHERE move_typ = 'H' -- Move to history (release)
            AND loc_cd = '918' -- Probation early discharge
        -- dedup for ~15 records that have duplicate movements within the same docno/incrno
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY docno, incrno
            ORDER BY move_dtd DESC
        ) = 1
    ),
    requests_discharges AS (
        -- This cte joins request data to discharge data. It keeps the most recent discharge date when discharges are found in both parole and probation data.
        -- The join between request and discharge is limited to only the most recent request on a given sentence, and must be between 1 month before or up
        -- to x months after the date the request was created
        SELECT
            state.*,
            GREATEST(COALESCE(parole.sent_sat_dtd, probation.move_dtd), COALESCE(probation.move_dtd, parole.sent_sat_dtd)) AS discharge_date,
        FROM state_ed_requests_sentences state
        LEFT JOIN raw_parole_discharges parole
            ON state.mitt_srl = parole.mitt_srl
            AND state.sent_no = parole.sent_no
            AND state.request_sentence_recency_rank = 1
            AND DATE_DIFF(parole.sent_sat_dtd, state.create_date, MONTH) BETWEEN -1 AND CAST({parole_ed_months} AS INT64)
        LEFT JOIN raw_probation_discharges probation
            ON state.mitt_srl = probation.mitt_srl
            AND state.caseno_seq = probation.caseno_seq
            AND state.request_sentence_recency_rank = 1
            AND DATE_DIFF(probation.move_dtd, state.create_date, MONTH) BETWEEN -1 AND CAST({probation_ed_months} AS INT64)
    ),
    requests_discharges_flags AS (
        -- This cte adds flags to indicate when decision information must be inferred based on the existence of an early discharge,
        -- which sometimes conflicts with the data entered manually by POs.
        SELECT
            *,
            -- Flag for outdated requests that are still marked pending
            CASE
                WHEN decision_status_raw = 'PENDING' THEN
                    CASE
                        -- Flag probation requests after x months
                        WHEN supervision_type = 'PROBATION' AND DATE_DIFF(CURRENT_DATE(), create_date, MONTH) >= CAST({probation_ed_months} AS INT64) THEN 1
                        -- Flag parole requests after x months
                        WHEN supervision_type = 'PAROLE' AND DATE_DIFF(CURRENT_DATE(), create_date, MONTH) >= CAST({parole_ed_months} AS INT64)THEN 1
                        ELSE 0
                    END
                ELSE 0
            END AS flag_outdated,
            -- Flag for decision_status disagreeing with identified discharge
            CASE WHEN decision_status_raw = 'PENDING' AND discharge_date IS NOT NULL THEN 1 ELSE 0 END AS flag_status_mismatch,
            -- Flag for decision disagreeing with identified discharge
            CASE WHEN COALESCE(decision_raw, '') NOT LIKE '%GRANTED%' AND discharge_date IS NOT NULL THEN 1 ELSE 0 END AS flag_decision_mismatch,
            -- Flag for unsubmitted requests
            CASE WHEN submit_date IS NULL THEN 1 ELSE 0 END AS flag_unsubmitted,
        FROM requests_discharges
    ),
    requests_inference AS (
        -- Cleanup and adds some inferred fields based on flags from previous cte
        SELECT
            *,
            -- Finally, set some simple flags to indicate whether or not we have inferred data
            CASE WHEN COALESCE(decision_date_raw, '9999-01-01') != COALESCE(decision_date, '9999-01-01') THEN 1 ELSE 0 END AS decision_date_is_inferred,
            CASE WHEN COALESCE(decision_status_raw, '') != COALESCE(decision_status, '') THEN 1 ELSE 0 END AS decision_status_is_inferred,
            CASE WHEN COALESCE(decision_raw, '') != COALESCE(decision, '') THEN 1 ELSE 0 END AS decision_is_inferred,
        FROM (
            SELECT
                *,
                CASE
                    -- If there's a date entered, use it
                    WHEN decision_date_raw IS NOT NULL THEN decision_date_raw
                    -- If decision date is null but there is an identified discharge date, use the discharge date
                    WHEN discharge_date IS NOT NULL THEN discharge_date
                    -- If we've determined that the request is outdated, add our pre-determined buffer months
                    WHEN flag_outdated = 1 THEN DATE_ADD(create_date, INTERVAL CASE
                            WHEN supervision_type = 'PAROLE' THEN CAST({parole_ed_months} AS INT64)
                            WHEN supervision_type = 'PROBATION' THEN CAST({probation_ed_months} AS INT64)
                        END MONTH)
                END AS decision_date,
                CASE
                    -- If either of these flags is set then we've determined that the request actually has been decided
                    WHEN flag_outdated = 1 OR flag_status_mismatch = 1 THEN 'DECIDED'
                    ELSE decision_status_raw
                END AS decision_status,
                CASE
                    -- If the decision mismatch flag is set, then we have inferred a granted discharge
                    WHEN flag_decision_mismatch = 1 THEN 'SENTENCE_TERMINATION_GRANTED'
                    -- If we've inferred that the request is outdated, mark it as denied
                    WHEN flag_outdated = 1 THEN 'REQUEST_DENIED'
                    ELSE decision_raw
                END AS decision,
                CASE
                    -- Same logic as above
                    WHEN flag_decision_mismatch = 1 THEN 'GRANT_REQUEST_TO_TERMINATE'
                    WHEN flag_outdated = 1 THEN 'DENY'
                    ELSE decision_reason_raw
                END AS decision_reason,
            FROM requests_discharges_flags 
        )
    )

    SELECT *
    FROM requests_inference
    """

US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_NAME,
    view_query_template=US_ID_EARLY_DISCHARGE_REQUESTS_QUERY_TEMPLATE,
    description=US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_DESCRIPTION,
    state_dataset=STATE_BASE_DATASET,
    us_id_raw_dataset=US_ID_RAW_DATASET,
    parole_ed_months=PAROLE_ED_MONTHS,
    probation_ed_months=PROBATION_ED_MONTHS,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER.build_and_print()
