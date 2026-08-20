# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Exemptions for existing deployed view queries that reference legacy TOMIS 1.0
raw data (raw data tables, *_latest views, or *_all views) and have not yet been
migrated to MiCase (TOMIS 2.0) data.

Do NOT add new entries to this list unless absolutely necessary -- new views
should read from MiCase raw data instead. Entries are removed as views are
migrated; the burndown test in us_tn_tomis_migration_burndown_test.py fails if
an entry is stale (i.e. the reference no longer exists) so that this list can
only shrink over time.
"""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_contact_comments_preprocessed import (
    US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_exemptions_preprocessed import (
    US_TN_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_incarceration_incidents_preprocessed import (
    US_TN_INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_invoices_preprocessed import (
    US_TN_INVOICES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_jii_raw_data_update_date import (
    US_TN_JII_RAW_DATA_UPDATE_DATE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_jii_tools_date_aligned__OffenderCredit_latest import (
    US_TN_JII_TOOLS_DATE_ALIGNED_OFFENDER_CREDIT_LATEST_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_jii_tools_date_aligned__OffenderSentenceSummary_latest import (
    US_TN_JII_TOOLS_DATE_ALIGNED_OFFENDER_SENTENCE_SUMMARY_LATEST_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_payments_preprocessed import (
    US_TN_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_prior_record_preprocessed import (
    US_TN_PRIOR_RECORD_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_relevant_contact_codes import (
    US_TN_RELEVANT_CONTACT_CODES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_segregation_lists import (
    US_TN_SEGREGATION_LISTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_segregation_stays import (
    US_TN_SEGREGATION_STAYS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_zero_tolerance_codes import (
    US_TN_ZERO_TOLERANCE_CODES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.classification.score_components.us_tn.caf_q8 import (
    VIEW_BUILDER as CAF_Q8_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.classification.score_components.us_tn.dcaf_rcaf_q7_v1 import (
    VIEW_BUILDER as DCAF_RCAF_Q7_V1_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.supervision_to_prison_population_snapshot_by_officer import (
    SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_client_events import (
    SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.location_metadata.us_tn_location_metadata import (
    US_TN_LOCATION_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_drug_screens_preprocessed import (
    US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_parole_board_hearing_decisions import (
    US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_parole_board_hearing_sessions import (
    US_TN_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_sentences_preprocessed import (
    US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_annual_reclassification_review_2026_policy_record import (
    US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_annual_reclassification_review_2026_policy_v2_record import (
    US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_annual_reclassification_review_2026_policy_v3_record import (
    US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_annual_reclassification_review_record import (
    US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_custody_level_downgrade_2026_policy_record import (
    US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_custody_level_downgrade_2026_policy_v2_record import (
    US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_custody_level_downgrade_2026_policy_v3_record import (
    US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_custody_level_downgrade_record import (
    US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_full_term_supervision_discharge_record import (
    US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_initial_classification_review_2026_policy_record import (
    US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_initial_classification_review_record import (
    US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_special_custody_level_upgrade_2026_policy_record import (
    US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_special_custody_level_upgrade_2026_policy_v2_record import (
    US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_special_custody_level_upgrade_2026_policy_v3_record import (
    US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_suspension_of_direct_supervision_record import (
    US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_transfer_to_compliant_reporting_2025_policy_record import (
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.most_recent_fee_code_is_feep_in_last_90_days import (
    VIEW_BUILDER as MOST_RECENT_FEE_CODE_IS_FEEP_IN_LAST_90_DAYS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.negative_arrest_check_in_past_6_months import (
    VIEW_BUILDER as NEGATIVE_ARREST_CHECK_IN_PAST_6_MONTHS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.negative_arrest_check_in_past_year import (
    VIEW_BUILDER as NEGATIVE_ARREST_CHECK_IN_PAST_YEAR_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.no_arrests_in_past_2_years import (
    VIEW_BUILDER as NO_ARRESTS_IN_PAST_2_YEARS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.no_arrests_in_past_6_months import (
    VIEW_BUILDER as NO_ARRESTS_IN_PAST_6_MONTHS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.no_arrests_in_past_year import (
    VIEW_BUILDER as NO_ARRESTS_IN_PAST_YEAR_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.no_arrests_since_intake_supervision_level import (
    VIEW_BUILDER as NO_ARRESTS_SINCE_INTAKE_SUPERVISION_LEVEL_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.no_high_sanctions_in_past_year import (
    VIEW_BUILDER as NO_HIGH_SANCTIONS_IN_PAST_YEAR_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.no_recent_compliant_reporting_rejections import (
    VIEW_BUILDER as NO_RECENT_COMPLIANT_REPORTING_REJECTIONS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.no_warrant_within_2_years import (
    VIEW_BUILDER as NO_WARRANT_WITHIN_2_YEARS_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_permanently_rejected_from_compliant_reporting import (
    VIEW_BUILDER as NOT_PERMANENTLY_REJECTED_FROM_COMPLIANT_REPORTING_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.special_conditions_are_current import (
    VIEW_BUILDER as SPECIAL_CONDITIONS_ARE_CURRENT_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_tn.incarceration_population_person_level import (
    US_TN_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.external_data.regions.us_tn.supervision_population_person_level import (
    US_TN_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.state.analyst_data_validation.us_tn.us_tn_classification_2026_policy_without_recent_form_download import (
    US_TN_CLASSIFICATION_2026_POLICY_WITHOUT_RECENT_FORM_DOWNLOAD_VIEW_BUILDER,
)
from recidiviz.validation.views.state.analyst_data_validation.us_tn.us_tn_loopback_missing_classifications import (
    US_TN_LOOPBACK_MISSING_CLASSIFICATIONS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.analyst_data_validation.us_tn.us_tn_loopback_wonky_recidiviz_data import (
    US_TN_LOOPBACK_WONKY_RECIDIVIZ_DATA_VIEW_BUILDER,
)
from recidiviz.validation.views.state.analyst_data_validation.us_tn.us_tn_loopback_wonky_tomis_data import (
    US_TN_LOOPBACK_WONKY_TOMIS_DATA_VIEW_BUILDER,
)

# Map of each legacy TOMIS 1.0 BigQuery address to the deployed views that still
# reference it directly, with a reason (generally a TO-DO for the migration work).
US_TN_LEGACY_TOMIS_REFERENCE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.BoardAction_latest"): {
        US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER.address: "TODO(TN-1969): Migrate this reference off of legacy TOMIS 1.0 raw data",
        CLIENT_RECORD_VIEW_BUILDER.address: "TODO(TN-1970): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.CAFScore_latest"): {
        US_TN_CLASSIFICATION_2026_POLICY_WITHOUT_RECENT_FORM_DOWNLOAD_VIEW_BUILDER.address: "TODO(TN-1971): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_LOOPBACK_MISSING_CLASSIFICATIONS_VIEW_BUILDER.address: "TODO(TN-1972): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_LOOPBACK_WONKY_TOMIS_DATA_VIEW_BUILDER.address: "TODO(TN-1973): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.ClassTerminationRequest_latest"
    ): {
        DCAF_RCAF_Q7_V1_VIEW_BUILDER.address: "TODO(TN-1930): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Classification_latest"): {
        US_TN_CLASSIFICATION_2026_POLICY_WITHOUT_RECENT_FORM_DOWNLOAD_VIEW_BUILDER.address: "TODO(TN-1974): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_LOOPBACK_MISSING_CLASSIFICATIONS_VIEW_BUILDER.address: "TODO(TN-1975): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_LOOPBACK_WONKY_RECIDIVIZ_DATA_VIEW_BUILDER.address: "TODO(TN-1976): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_LOOPBACK_WONKY_TOMIS_DATA_VIEW_BUILDER.address: "TODO(TN-1977): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-1978): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.CodesDescription_latest"
    ): {
        CLIENT_RECORD_VIEW_BUILDER.address: "TODO(TN-1979): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-1983): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-1983): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address: "TODO(TN-1980): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-1983): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address: "TODO(TN-1981): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-1982): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.ContactNoteComment_latest"
    ): {
        US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-1984): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.ContactNoteType_latest"
    ): {
        US_TN_RELEVANT_CONTACT_CODES_VIEW_BUILDER.address: "TODO(TN-1985): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ZERO_TOLERANCE_CODES_VIEW_BUILDER.address: "TODO(TN-1986): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-1987): Migrate this reference off of legacy TOMIS 1.0 raw data",
        MOST_RECENT_FEE_CODE_IS_FEEP_IN_LAST_90_DAYS_VIEW_BUILDER.address: "TODO(TN-1988): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NEGATIVE_ARREST_CHECK_IN_PAST_6_MONTHS_VIEW_BUILDER.address: "TODO(TN-1989): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NEGATIVE_ARREST_CHECK_IN_PAST_YEAR_VIEW_BUILDER.address: "TODO(TN-1990): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NO_ARRESTS_IN_PAST_2_YEARS_VIEW_BUILDER.address: "TODO(TN-1991): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NO_ARRESTS_IN_PAST_6_MONTHS_VIEW_BUILDER.address: "TODO(TN-1993): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NO_ARRESTS_IN_PAST_YEAR_VIEW_BUILDER.address: "TODO(TN-1991): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NO_ARRESTS_SINCE_INTAKE_SUPERVISION_LEVEL_VIEW_BUILDER.address: "TODO(TN-1994): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NO_RECENT_COMPLIANT_REPORTING_REJECTIONS_VIEW_BUILDER.address: "TODO(TN-1995): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NO_WARRANT_WITHIN_2_YEARS_VIEW_BUILDER.address: "TODO(TN-1996): Migrate this reference off of legacy TOMIS 1.0 raw data",
        NOT_PERMANENTLY_REJECTED_FROM_COMPLIANT_REPORTING_VIEW_BUILDER.address: "TODO(TN-1997): Migrate this reference off of legacy TOMIS 1.0 raw data",
        SPECIAL_CONDITIONS_ARE_CURRENT_VIEW_BUILDER.address: "TODO(TN-1998): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2000): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-1999): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.DailyCommunitySupervisionForRecidiviz_latest"
    ): {
        US_TN_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.address: "TODO(TN-2001): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Detainer_latest"): {
        CAF_Q8_VIEW_BUILDER.address: "TODO(TN-2002): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2003): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2003): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2003): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Disciplinary_latest"): {
        US_TN_INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2004): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.DrugTestDrugClass_latest"
    ): {
        US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2005): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.HealthExam_latest"): {
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2009): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Hearing_latest"): {
        US_TN_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER.address: "TODO(TN-2010): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.IncompatiblePair_latest"
    ): {
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2011): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.JOIdentification_latest"
    ): {
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2013): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.JOSentence_latest"): {
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2014): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.JOVictim_latest"): {
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2015): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.JobTerminationRequest_latest"
    ): {
        DCAF_RCAF_Q7_V1_VIEW_BUILDER.address: "TODO(TN-2012): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.MentalHealthServices_latest"
    ): {
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2016): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.OffenderAccounts_latest"
    ): {
        US_TN_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2018): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INVOICES_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2019): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_PAYMENTS_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2019): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.OffenderAttributes_latest"
    ): {
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2020): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.OffenderExemptions_latest"
    ): {
        US_TN_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2023): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.OffenderInvoices_latest"
    ): {
        US_TN_INVOICES_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2024): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.OffenderPayments_latest"
    ): {
        US_TN_PAYMENTS_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2025): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.OffenderSentenceSummary_latest"
    ): {
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2028): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.OffenderStatute_latest"
    ): {
        US_TN_PRIOR_RECORD_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2029): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2030): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address: "TODO(TN-2031): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.OffenderTreatment_latest"
    ): {
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.address: "TODO(TN-2032): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Offender_latest"): {
        CLIENT_RECORD_VIEW_BUILDER.address: "TODO(TN-2017): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.PriorRecord_latest"): {
        US_TN_PRIOR_RECORD_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2033): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.STGOffender_latest"): {
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2043): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Sanctions_latest"): {
        NO_HIGH_SANCTIONS_IN_PAST_YEAR_VIEW_BUILDER.address: "TODO(TN-2035): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Segregation_latest"): {
        US_TN_SEGREGATION_LISTS_VIEW_BUILDER.address: "TODO(TN-2036): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SEGREGATION_STAYS_VIEW_BUILDER.address: "TODO(TN-2037): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2038): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Sentence_latest"): {
        US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: "TODO(TN-2039): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Site_latest"): {
        US_TN_LOCATION_METADATA_VIEW_BUILDER.address: "TODO(TN-2040): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address: "TODO(TN-2041): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.TDPOP_latest"): {
        US_TN_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.address: "TODO(TN-2046): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.TOMIS_CODESTABLE_latest"
    ): {
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.address: "TODO(TN-2047): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.VantagePointAssessments_latest"
    ): {
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2048): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.VantagePointPathways_latest"
    ): {
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2049): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.VantagePointProgram_latest"
    ): {
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2050): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str(
        "us_tn_raw_data_up_to_date_views.VantagePointRecommendations_latest"
    ): {
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.address: "TODO(TN-2051): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V2_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_SPECIAL_CUSTODY_LEVEL_UPGRADE_2026_POLICY_V3_RECORD_VIEW_BUILDER.address: "TODO(TN-2052): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_up_to_date_views.Violations_latest"): {
        NO_HIGH_SANCTIONS_IN_PAST_YEAR_VIEW_BUILDER.address: "TODO(TN-2053): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_views.AssignedStaff_all"): {
        SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_BUILDER.address: "TODO(TN-1968): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_views.OffenderCredit_all"): {
        US_TN_JII_RAW_DATA_UPDATE_DATE_VIEW_BUILDER.address: "TODO(TN-2021): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_JII_TOOLS_DATE_ALIGNED_OFFENDER_CREDIT_LATEST_VIEW_BUILDER.address: "TODO(TN-2022): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_views.OffenderSentenceSummary_all"): {
        US_TN_JII_RAW_DATA_UPDATE_DATE_VIEW_BUILDER.address: "TODO(TN-2026): Migrate this reference off of legacy TOMIS 1.0 raw data",
        US_TN_JII_TOOLS_DATE_ALIGNED_OFFENDER_SENTENCE_SUMMARY_LATEST_VIEW_BUILDER.address: "TODO(TN-2027): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
    BigQueryAddress.from_str("us_tn_raw_data_views.SupervisionPlan_all"): {
        SUPERVISION_TO_PRISON_POPULATION_SNAPSHOT_BY_OFFICER_VIEW_BUILDER.address: "TODO(TN-2045): Migrate this reference off of legacy TOMIS 1.0 raw data",
    },
}
