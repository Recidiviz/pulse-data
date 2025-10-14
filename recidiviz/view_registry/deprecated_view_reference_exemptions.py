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
from recidiviz.calculator.query.state.views.sentencing.recidivism_event import (
    RECIDIVISM_EVENT_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_discharge_early_from_supervision_request_record import (
    US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_transfer_to_limited_supervision_form_record import (
    US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_custody_reclassification_review_form_record import (
    US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_pa_complete_transfer_to_special_circumstances_supervision_request_record import (
    US_PA_COMPLETE_TRANSFER_TO_SPECIAL_CIRCUMSTANCES_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_pa_transfer_to_administrative_supervision_form_record import (
    US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_annual_reclassification_review_record import (
    US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_custody_level_downgrade_record import (
    US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_full_term_supervision_discharge_record import (
    US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_initial_classification_review_record import (
    US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
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
from recidiviz.observations.views.spans.person.sentence_span import (
    VIEW_BUILDER as SENTENCE_SPAN_OBSERVATIONS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.incarceration_past_half_full_term_release_date import (
    VIEW_BUILDER as INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER,
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
from recidiviz.task_eligibility.criteria.state_specific.us_pa.meets_special_circumstances_criteria_for_time_served import (
    VIEW_BUILDER as US_PA_MEETS_SPECIAL_CIRCUMSTANCES_CRITERIA_FOR_TIME_SERVED_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_pa.not_serving_ineligible_offense_for_admin_supervision import (
    VIEW_BUILDER as US_PA_NOT_SERVING_INELIGIBLE_OFFENSE_FOR_ADMIN_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_on_community_supervision_for_life import (
    VIEW_BUILDER as US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_on_life_sentence_or_lifetime_supervision import (
    VIEW_BUILDER as US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_serving_ineligible_cr_offense import (
    VIEW_BUILDER as US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_serving_ineligible_cr_offense_policy_b import (
    VIEW_BUILDER as US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_serving_unknown_cr_offense import (
    VIEW_BUILDER as US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER,
)
from recidiviz.tools.find_unused_bq_views import PSA_RISK_SCORES_VIEW_BUILDER
from recidiviz.validation.views.state.sentences.sentences_missing_date_imposed import (
    SENTENCES_MISSING_DATE_IMPOSED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.session_liberty_releases_with_no_sentence_completion_date import (
    SESSION_LIBERTY_RELEASES_WITH_NO_SENTENCE_COMPLETION_DATE_VIEW_BUILDER,
)

# Maps deprecated view addresses to a dict of views that are allowed to reference
# them. The inner dict maps each exempted view address to a reason string explaining
# why the reference still exists.
DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {
    # TODO(#33402): deprecate `sentences_preprocessed` once all states are migrated
    # to v2 infra
    SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
        PSA_RISK_SCORES_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view (or delete this view)"
        ),
        SENTENCE_SPAN_OBSERVATIONS_VIEW_BUILDER.address: (
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
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
            "TODO(#33402): Replace this reference with a reference to a "
            "sentence_sessions view"
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
        US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
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
            "TODO(#46260): Remove this reference as part of the v2 sentences migration"
        ),
        US_PA_TRANSFER_TO_ADMINISTRATIVE_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: (
            "TODO(#46260): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address: (
            "TODO(#46261): Remove this reference as part of the v2 sentences migration"
        ),
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: (
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
}
