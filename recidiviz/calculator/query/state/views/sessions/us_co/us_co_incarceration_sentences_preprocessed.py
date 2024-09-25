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
"""Processed Incarceration Sentencing Data for US_CO"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_CO_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_NAME = (
    "us_co_incarceration_sentences_preprocessed"
)

US_CO_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION = (
    """Processed Incarceration Sentencing Data for US_CO"""
)

# TODO(#16173): Remove this preprocessing file once ingest handles this logic
US_CO_INCARCERATION_SENTENCES_PREPROCESSED_QUERY_TEMPLATE = """
    WITH date_imposed AS
    (
    /*
    Pull date imposed from the raw sentencing file
    */
    SELECT
        pei.state_code,
        pei.person_id,
        commitment.OFFENDERID || "-" || commitment.COMMITMENTPREFIX || "-" || component.SENTENCECOMPONENT AS external_id,
        SAFE.PARSE_DATE('%Y-%m-%d', commitment.INCARCERATIONSTART) AS date_imposed,
    FROM `{project_id}.{raw_dataset}.eomis_commitmentsummary_latest` commitment
    INNER JOIN `{project_id}.{raw_dataset}.eomis_sentencecomponent_latest` component
        USING (OFFENDERID, COMMITMENTPREFIX)
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON commitment.OFFENDERID = pei.external_id
        AND pei.id_type = "US_CO_PID"
    )
    SELECT
        sis.person_id,
        sis.state_code,
        sis.incarceration_sentence_id AS sentence_id,
        sis.external_id AS external_id,
        'INCARCERATION' AS sentence_type,
        'INCARCERATION' AS sentence_sub_type,
        sis.effective_date,
        di.date_imposed,
        -- Update when `completion_date` is no longer the same as `effective_date`
        CAST(NULL AS DATE) AS completion_date,
        sis.status,
        sis.status_raw_text,
        sis.parole_eligibility_date,
        sis.projected_min_release_date AS projected_completion_date_min,
        sis.projected_max_release_date AS projected_completion_date_max,
        sis.initial_time_served_days,
        COALESCE(sis.is_life, FALSE) AS life_sentence,
        sis.min_length_days,
        sis.max_length_days,
        sis.county_code,
        sis.sentence_metadata,
        charge.* EXCEPT(person_id, state_code, external_id, status, status_raw_text, county_code)
    FROM `{project_id}.{normalized_state_dataset}.state_incarceration_sentence` AS sis
    LEFT JOIN `{project_id}.{normalized_state_dataset}.state_charge_incarceration_sentence_association` assoc
        ON assoc.state_code = sis.state_code
        AND assoc.incarceration_sentence_id = sis.incarceration_sentence_id
    LEFT JOIN `{project_id}.{sessions_dataset}.charges_preprocessed` charge
        ON charge.state_code = assoc.state_code
        AND charge.charge_id = assoc.charge_id
    LEFT JOIN date_imposed di
        ON di.state_code = sis.state_code
        AND di.person_id = sis.person_id
        AND di.external_id = sis.external_id
    WHERE sis.external_id IS NOT NULL
        AND sis.state_code = "US_CO"
"""

US_CO_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_CO_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_NAME,
    view_query_template=US_CO_INCARCERATION_SENTENCES_PREPROCESSED_QUERY_TEMPLATE,
    description=US_CO_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_DESCRIPTION,
    raw_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_CO, instance=DirectIngestInstance.PRIMARY
    ),
    sessions_dataset=SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CO_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER.build_and_print()
