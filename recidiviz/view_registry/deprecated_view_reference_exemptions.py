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
"""Lists views that are currently exempted from checks preventing references to
deprecated views.
"""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_early_discharge_sessions_preprocessing import (
    US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q1 import (
    US_IX_SLS_Q1_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q3 import (
    US_IX_SLS_Q3_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_nd.us_nd_early_discharge_sessions_preprocessing import (
    US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.liberty_to_prison_transitions import (
    LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_projected_date_sessions_v1_states import (
    SENTENCE_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_serving_start_date import (
    SENTENCE_SERVING_START_DATE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_status_raw_text_sessions import (
    SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentences_and_charges import (
    SENTENCES_AND_CHARGES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.case_disposition import (
    SENTENCING_CASE_DISPOSITION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.charge_record import (
    SENTENCING_CHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.recidivism_event import (
    RECIDIVISM_EVENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.sentence_cohort import (
    SENTENCE_COHORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.charges_preprocessed import (
    CHARGES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions_closest_sentence_imposed_group import (
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.consecutive_sentences_preprocessed import (
    CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_projected_completion_date_spans import (
    INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_deadline_spans import (
    SENTENCE_DEADLINE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_relationship import (
    SENTENCE_RELATIONSHIP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_spans import (
    SENTENCE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentences_preprocessed import (
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_projected_completion_date_spans import (
    SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ix.us_ix_consecutive_sentences_preprocessed import (
    US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_me.us_me_consecutive_sentences_preprocessed import (
    US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_nd.us_nd_consecutive_sentences_preprocessed import (
    US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_nd.us_nd_incarceration_sentences_preprocessed import (
    US_ND_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_consecutive_sentences_preprocessed import (
    US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_sentence_status_raw_text_sessions import (
    US_TN_SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_sentences_preprocessed import (
    US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.single_day_incarceration_population_for_spotlight import (
    SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.single_day_supervision_population_for_spotlight import (
    SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_terminations_for_spotlight import (
    SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_discharge_early_from_supervision_request_record import (
    US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_transfer_to_limited_supervision_form_record import (
    US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_crc_resident_worker_request_record import (
    US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_crc_work_release_request_record import (
    US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_xcrc_request_record import (
    US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_custody_reclassification_review_form_record import (
    US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_nd_complete_discharge_early_from_supervision_record import (
    US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_pa_complete_transfer_to_special_circumstances_supervision_request_record import (
    US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_pa_transfer_to_administrative_supervision_form_record import (
    US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_full_term_supervision_discharge_record import (
    US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_suspension_of_direct_supervision_record import (
    US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_transfer_to_compliant_reporting_2025_policy_record import (
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_transfer_to_compliant_reporting_record import (
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ar.resident_metadata import (
    US_AR_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.views.unioned_normalized_state_views import (
    normalized_state_view_address_for_entity_table,
)
from recidiviz.ingest.views.view_config import (
    get_view_builders_for_views_to_update as get_ingest_infra_view_builders,
)
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.monitoring.platform_kpis.velocity.normalized_state_hydration_live_snapshot import (
    NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID,
)
from recidiviz.observations.views.events.person.sentences_imposed import (
    VIEW_BUILDER as SENTENCES_IMPOSED_OBSERVATIONS_VIEW_BUILDER,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.task_eligibility.criteria.general.incarceration_past_half_full_term_release_date import (
    VIEW_BUILDER as INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.incarceration_within_1_year_of_full_term_completion_date import (
    VIEW_BUILDER as INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.incarceration_within_3_months_of_full_term_completion_date import (
    VIEW_BUILDER as INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.incarceration_within_42_months_of_full_term_completion_date import (
    VIEW_BUILDER as INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.no_escape_in_current_incarceration import (
    VIEW_BUILDER as NO_ESCAPE_IN_CURRENT_INCARCERATION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.serving_at_least_one_year_on_parole_supervision_or_supervision_out_of_state import (
    VIEW_BUILDER as SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.serving_incarceration_sentence_of_less_than_6_years import (
    VIEW_BUILDER as SERVING_INCARCERATION_SENTENCE_OF_LESS_THAN_6_YEARS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.supervision_early_discharge_before_full_term_completion_date import (
    VIEW_BUILDER as SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ar.eligible_criminal_history_309 import (
    VIEW_BUILDER as US_AR_ELIGIBLE_CRIMINAL_HISTORY_309_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ar.eligible_criminal_history_work_release import (
    VIEW_BUILDER as US_AR_ELIGIBLE_CRIMINAL_HISTORY_WORK_RELEASE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_arson_conviction import (
    VIEW_BUILDER as US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_dangerous_crimes_against_children_conviction import (
    VIEW_BUILDER as US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_domestic_violence_conviction import (
    VIEW_BUILDER as US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_ineligible_offense_conviction_for_admin_supervision import (
    VIEW_BUILDER as US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_sexual_exploitation_of_children_conviction import (
    VIEW_BUILDER as US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_sexual_offense_conviction import (
    VIEW_BUILDER as US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_violent_conviction import (
    VIEW_BUILDER as US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.no_violent_conviction_unless_assault_or_aggravated_assault_or_robbery_conviction import (
    VIEW_BUILDER as US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.not_serving_flat_sentence import (
    VIEW_BUILDER as US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.not_serving_ineligible_offense_for_admin_supervision import (
    VIEW_BUILDER as US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_az.only_drug_offense_convictions import (
    VIEW_BUILDER as US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_me.supervision_past_half_full_term_release_date_from_probation_start import (
    VIEW_BUILDER as US_ME_SUPERVISION_PAST_HALF_FULL_TERM_RELEASE_DATE_FROM_PROBATION_START_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_nd.no_escape_offense_within_1_year import (
    VIEW_BUILDER as US_ND_NO_ESCAPE_OFFENSE_WITHIN_1_YEAR_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_pa.meets_special_circumstances_criteria_for_time_served import (
    VIEW_BUILDER as US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_pa.not_serving_ineligible_offense_for_admin_supervision import (
    VIEW_BUILDER as US_PA_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.tools.find_unused_bq_views import PSA_RISK_SCORES_VIEW_BUILDER
from recidiviz.utils.types import assert_subclass
from recidiviz.validation.configured_validations import (
    NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_BUILDER,
    SENTENCE_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.primary_keys_unique_across_all_states import (
    PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.normalized_state_charge_missing_uniform_offense_labels import (
    NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.sentences_missing_date_imposed import (
    SENTENCES_MISSING_DATE_IMPOSED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.sentences_undefined_relationship import (
    SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.session_liberty_releases_with_no_sentence_completion_date import (
    SESSION_LIBERTY_RELEASES_WITH_NO_SENTENCE_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.sessions_missing_closest_sentence_imposed_group import (
    SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.us_mi_flag_new_offense_codes import (
    US_MI_FLAG_NEW_OFFENSE_CODES_VIEW_BUILDER,
)


def _get_ingest_metadata_addresses_table(
    state_table_id: str,
) -> list[BigQueryAddress]:
    """Returns addresses for all ingest_metadata views that reference the given table."""
    addresses = []
    table_prefix = f"ingest_state_metadata__{state_table_id}__"
    for builder in get_ingest_infra_view_builders():
        if builder.address.table_id.startswith(table_prefix):
            addresses.append(builder.address)
    return addresses


_ALL_SCHEMA_TABLE_VIEWS = [
    BigQueryAddress(
        dataset_id=PLATFORM_KPIS_DATASET,
        table_id=NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID,
    ),
    PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER.address,
]

# These are views that still reference state-specific sentences v1 tables and have been
# exempted. For example the views listed under (StateCode.US_IX, StateCharge) are views
# that reference the us_ix_normalized_state.state_charge table directly.
_SENTENCE_STATE_SPECIFIC_REFERENCE_EXEMPTIONS = {
    (StateCode.US_IX, StateCharge): {
        US_IX_SLS_Q1_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        US_IX_SLS_Q3_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        SENTENCING_CHARGE_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
    },
    (StateCode.US_IX, StateIncarcerationSentence): {
        US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_incarceration_sentence reference as "
            "part of the v2 sentences migration"
        ),
    },
    (StateCode.US_IX, StateSupervisionSentence): {
        US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46255): Remove state_supervision_sentence reference as part "
            "of the v2 sentences migration"
        ),
    },
    (StateCode.US_ME, StateCharge): {
        US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46256): Remove state_supervision_sentence reference as part "
            "of the v2 sentences migration"
        ),
    },
    (StateCode.US_ME, StateIncarcerationSentence): {
        RESIDENT_RECORD_VIEW_BUILDER.address: (
            "TODO(#46256): Remove state_incarceration_sentence reference as "
            "part of the v2 sentences migration"
        ),
        US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46256): Remove state_incarceration_sentence reference as "
            "part of the v2 sentences migration"
        ),
    },
    (StateCode.US_ME, StateSupervisionSentence): {
        US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46256): Remove state_supervision_sentence reference as part "
            "of the v2 sentences migration"
        ),
        US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46256): Remove state_supervision_sentence reference as part "
            "of the v2 sentences migration"
        ),
    },
    (StateCode.US_ND, StateCharge): {
        US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address: (
            "TODO(#46257): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        US_ND_NO_ESCAPE_OFFENSE_WITHIN_1_YEAR_VIEW_BUILDER.address: (
            "TODO(#46257): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
    },
    (StateCode.US_ND, StateIncarcerationSentence): {
        US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46257): This view should be deleted once ND no longer "
            "relies on v1 sentences"
        ),
        US_ND_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46257): This view should be deleted once ND no longer "
            "relies on v1 sentences"
        ),
    },
    (StateCode.US_ND, StateSupervisionSentence): {
        US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address: (
            "TODO(#46257): Remove state_supervision_sentence reference as part "
            "of the v2 sentences migration"
        ),
        US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address: (
            "TODO(#46257): Remove state_supervision_sentence reference as part "
            "of the v2 sentences migration"
        ),
    },
    (StateCode.US_PA, StateCharge): {
        US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#50859): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
        US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#50859): Remove state_charge reference as part of the v2 "
            "sentences migration"
        ),
    },
    (StateCode.US_TN, StateCharge): {
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46261): This view should be deleted once TN no longer "
            "relies on v1 sentences"
        ),
    },
    (StateCode.US_TN, StateIncarcerationSentence): {
        CLIENT_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove state_incarceration_sentence reference as "
            "part of the v2 sentences migration"
        ),
        US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46261): This view should be deleted once TN no longer "
            "relies on v1 sentences"
        ),
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46261): This view should be deleted once TN no longer "
            "relies on v1 sentences"
        ),
    },
    (StateCode.US_TN, StateSupervisionSentence): {
        CLIENT_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove state_supervision_sentence reference as part "
            "of the v2 sentences migration"
        ),
        US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46261): This view should be deleted once TN no longer "
            "relies on v1 sentences"
        ),
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46261): This view should be deleted once TN no longer "
            "relies on v1 sentences"
        ),
    },
    (StateCode.US_MI, StateCharge): {
        US_MI_FLAG_NEW_OFFENSE_CODES_VIEW_BUILDER.address: (
            "TODO(#46258): Update validation to read from state_charge_v2"
        ),
    },
}

# Maps deprecated view addresses to a dict of views that are allowed to reference
# them. The inner dict maps each exempted view address to a reason string explaining
# why the reference still exists.
SENTENCES_V1_DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {
    # TODO(#33402): Delete `sentences_preprocessed` once all states are migrated
    # to v2 infra
    SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        PSA_RISK_SCORES_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view (or delete this view)"
        ),
        SENTENCE_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted once no states rely on v1 sentences"
        ),
        SENTENCE_SERVING_START_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SENTENCES_AND_CHARGES_VIEW_BUILDER.address: (
            "TODO(#33402): This reference should go away once we no longer rely on v1 "
            "sentences."
        ),
        BigQueryAddress(
            dataset_id="sentence_sessions_v2_all",
            table_id="sentence_projected_date_sessions_v1_states",
        ): (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        BigQueryAddress(
            dataset_id="sentence_sessions_v2_all",
            table_id="sentence_serving_start_date",
        ): (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        BigQueryAddress(
            dataset_id="sentence_sessions_v2_all",
            table_id="sentence_status_raw_text_sessions",
        ): (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        BigQueryAddress(
            dataset_id="sentence_sessions_v2_all", table_id="sentences_and_charges"
        ): (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        RECIDIVISM_EVENT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCE_DEADLINE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCE_RELATIONSHIP_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        NO_ESCAPE_IN_CURRENT_INCARCERATION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SERVING_INCARCERATION_SENTENCE_OF_LESS_THAN_6_YEARS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AR_ELIGIBLE_CRIMINAL_HISTORY_309_VIEW_BUILDER.address: (
            "TODO(#46254): Remove this reference as part of the v2 sentences migration"
        ),
        US_AR_ELIGIBLE_CRIMINAL_HISTORY_WORK_RELEASE_VIEW_BUILDER.address: (
            "TODO(#46254): Remove this reference as part of the v2 sentences migration"
        ),
        US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED_VIEW_BUILDER.address: (
            "TODO(#46260): Remove this reference as part of the v2 sentences migration"
        ),
        US_PA_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address: (
            "TODO(#46260): Remove this reference as part of the v2 sentences migration"
        ),
        SENTENCES_MISSING_DATE_IMPOSED_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SESSION_LIBERTY_RELEASES_WITH_NO_SENTENCE_COMPLETION_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AR_RESIDENT_METADATA_VIEW_BUILDER.address: (
            "TODO(#46254): Remove this reference as part of the v2 sentences migration"
        ),
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
        US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
        US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46256): Remove this reference as part of the v2 sentences migration"
        ),
        US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#50859): Remove this reference as part of the v2 sentences migration"
        ),
        US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#50859): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
    },
    # TODO(#33402): Delete `sentence_spans` once all states are migrated
    # to v2 infra
    SENTENCE_SPANS_VIEW_BUILDER.address: {
        INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SERVING_INCARCERATION_SENTENCE_OF_LESS_THAN_6_YEARS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NOT_SERVING_FLAT_SENTENCE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED_VIEW_BUILDER.address: (
            "TODO(#50859): Remove this reference as part of the v2 sentences migration"
        ),
        US_AR_RESIDENT_METADATA_VIEW_BUILDER.address: (
            "TODO(#46254): Remove this reference as part of the v2 sentences migration"
        ),
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
        US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
        US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46256): Remove this reference as part of the v2 sentences migration"
        ),
        US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#50859): Remove this reference as part of the v2 sentences migration"
        ),
        US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#50859): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
    },
    # TODO(#33402): Delete `charges_preprocessed` once all states are migrated
    # to v2 infra
    CHARGES_PREPROCESSED_VIEW_BUILDER.address: {
        SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        SENTENCES_AND_CHARGES_VIEW_BUILDER.address: (
            "TODO(#33402): This reference should go away once we no longer rely on v1 "
            "sentences."
        ),
        BigQueryAddress(
            dataset_id="sentence_sessions_v2_all", table_id="sentences_and_charges"
        ): (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        US_AZ_NO_ARSON_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_DANGEROUS_CRIMES_AGAINST_CHILDREN_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_DOMESTIC_VIOLENCE_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_INELIGIBLE_OFFENSE_CONVICTION_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_SEXUAL_EXPLOITATION_OF_CHILDREN_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_SEXUAL_OFFENSE_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_VIOLENT_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NO_VIOLENT_CONVICTION_UNLESS_ASSAULT_OR_AGGRAVATED_ASSAULT_OR_ROBBERY_CONVICTION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_AZ_ONLY_DRUG_OFFENSE_CONVICTIONS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_ND_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46257): This view should be deleted once ND no longer "
            "relies on v1 sentences"
        ),
    },
    # TODO(#33402): Delete `sentence_deadline_spans` once all states are migrated
    # to v2 infra
    SENTENCE_DEADLINE_SPANS_VIEW_BUILDER.address: {
        SENTENCE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCE_PROJECTED_DATE_SESSIONS_V1_STATES_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        BigQueryAddress(
            dataset_id="sentence_sessions_v2_all",
            table_id="sentence_projected_date_sessions_v1_states",
        ): (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
    },
    # TODO(#33402): Delete `sentence_imposed_group_summary` once all states are
    # migrated to v2 infra
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
        SENTENCES_IMPOSED_OBSERVATIONS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        SENTENCE_SPANS_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
        LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        RECIDIVISM_EVENT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SENTENCE_COHORT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
    },
    # TODO(#33402): Delete `compartment_sessions_closest_sentence_imposed_group` once
    # all states are migrated to v2 infra
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
        LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SENTENCE_COHORT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
    },
    # TODO(#33402): Delete `sentence_relationship` once all states are migrated
    # to v2 infra
    SENTENCE_RELATIONSHIP_VIEW_BUILDER.address: {
        SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration - this validation is no longer necessary as all the same "
            "invariants are checked at ingest pipeline runtime."
        ),
    },
    # TODO(#33402): Delete `incarceration_projected_completion_date_spans` once all
    # states are migrated to v2 infra
    INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
        RESIDENT_RECORD_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
    },
    # TODO(#33402): Delete `supervision_projected_completion_date_spans` once all
    # states are migrated to v2 infra
    SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
        US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
        US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
        CLIENT_RECORD_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
        US_ME_SUPERVISION_PAST_HALF_FULL_TERM_RELEASE_DATE_FROM_PROBATION_START_VIEW_BUILDER.address: (
            "TODO(#46256): Remove this reference as part of the v2 sentences migration"
        ),
        SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
        ),
    },
    # TODO(#33402): Delete `consecutive_sentences_preprocessed` once all states are
    #  migrated to v2 infra
    CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration"
        ),
    },
    # TODO(#46255): Delete `us_ix_consecutive_sentences_preprocessed` once US_IX is
    #  migrated to v2 infra
    US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: (
            "TODO(#46255): Remove this reference as part of the v2 sentences migration"
        ),
    },
    # TODO(#46256): Delete `us_me_consecutive_sentences_preprocessed` once US_ME is
    #  migrated to v2 infra
    US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46256): Remove this reference as part of the v2 sentences migration"
        ),
    },
    # TODO(#46257): Delete `us_nd_consecutive_sentences_preprocessed` once US_ND is
    #  migrated to v2 infra
    US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46257): Remove this reference as part of the v2 sentences migration"
        ),
    },
    # TODO(#46261): Delete `us_tn_consecutive_sentences_preprocessed` once US_TN is
    #  migrated to v2 infra
    US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
    },
    # TODO(#46257): Delete `us_nd_consecutive_sentences_preprocessed` once US_ND is
    #  migrated to v2 infra
    US_ND_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46257): Remove this reference as part of the v2 sentences migration"
        ),
    },
    # TODO(#46261): Delete `us_tn_sentences_preprocessed` once US_TN is migrated to v2
    #  infra
    US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
    },
    # TODO(#46261): Delete `us_tn_sentence_status_raw_text_sessions` once US_TN is migrated to v2
    #  infra
    US_TN_SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER.address: {
        SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference when we flip the TN gate to v2"
        ),
        BigQueryAddress(
            dataset_id="sentence_sessions_v2_all",
            table_id="sentence_status_raw_text_sessions",
        ): "TODO(#46261): Remove this reference when we flip the TN gate to v2",
    },
    # TODO(#33402): Delete StateIncarcerationSentence once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(
        StateIncarcerationSentence.get_table_id()
    ): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when "
                "state_incarceration_sentence is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(
                    StateIncarcerationSentence.get_table_id()
                )
            )
        },
        SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration."
        ),
        SENTENCE_COMPARISON_VIEW_BUILDER.address: (
            "TODO(#33402): This view will no longer be needed once we have migrated "
            "all downstream views to read from sentences v2"
        ),
    },
    # TODO(#33402): Delete StateSupervisionSentence once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(
        StateSupervisionSentence.get_table_id()
    ): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when "
                "state_supervision_sentence is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(
                    StateSupervisionSentence.get_table_id()
                )
            )
        },
        SENTENCES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences "
            "migration."
        ),
        SENTENCE_COMPARISON_VIEW_BUILDER.address: (
            "TODO(#33402): This view will no longer be needed once we have migrated "
            "all downstream views to read from sentences v2"
        ),
    },
    # TODO(#33402): Delete StateCharge once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(StateCharge.get_table_id()): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when state_charge is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(StateCharge.get_table_id())
            )
        },
        CHARGES_PREPROCESSED_VIEW_BUILDER.address: (
            "TODO(#33402): This view should be deleted as part of the v2 sentences migration"
        ),
        NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_BUILDER.address: (
            "TODO(#33402): We need to make a state_charge_v2 equivalent of this "
            "validation, then delete this once all states are migrated to v2."
        ),
        NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_BUILDER.address: (
            "TODO(#33402): We need to make a state_charge_v2 equivalent of this "
            "validation, then delete this once all states are migrated to v2."
        ),
        SENTENCE_COMPARISON_VIEW_BUILDER.address: (
            "TODO(#33402): This view will no longer be needed once we have migrated "
            "all downstream views to read from sentences v2"
        ),
    },
    # State-specific v1 sentences table reference exemptions
    **{
        BigQueryAddress(
            dataset_id=normalized_state_dataset_for_state_code(state_code),
            table_id=assert_subclass(entity_cls, Entity).get_table_id(),
        ): (
            {
                normalized_state_view_address_for_entity_table(
                    assert_subclass(entity_cls, Entity).get_table_id()
                ): (
                    f"TODO(#33402): This view will be deleted when "
                    f"{assert_subclass(entity_cls, Entity).get_table_id()} is deleted"
                ),
                **_SENTENCE_STATE_SPECIFIC_REFERENCE_EXEMPTIONS.get(
                    (state_code, entity_cls), {}
                ),
            }
        )
        for state_code in get_existing_direct_ingest_states()
        if state_code != StateCode.US_OZ
        for entity_cls in [
            StateIncarcerationSentence,
            StateSupervisionSentence,
            StateCharge,
        ]
    },
}


# Maps deprecated view addresses to a dict of views that are allowed to reference
# them. The inner dict maps each exempted view address to a reason string explaining
# why the reference still exists.
DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {**SENTENCES_V1_DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS}
