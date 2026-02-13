# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Contains configured data validations to perform."""
import os
from functools import cache
from typing import Dict, List, Tuple

from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION
from recidiviz.validation import config
from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.checks.sameness_check import (
    SamenessDataValidationCheck,
    SamenessDataValidationCheckType,
)
from recidiviz.validation.config import regions as validation_regions
from recidiviz.validation.validation_config import (
    ValidationGlobalConfig,
    ValidationRegionConfig,
)
from recidiviz.validation.validation_models import (
    DataValidationCheck,
    ValidationCategory,
)
from recidiviz.validation.views.state.active_in_population_after_death_date import (
    ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.active_program_participation_by_region_internal_consistency import (
    ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.admission_pfi_pop_pfi_mismatch import (
    ADMISSION_PFI_POP_PFI_MISMATCH_VIEW_BUILDER,
)
from recidiviz.validation.views.state.analyst_data_validation.us_ma_resident_egt_discrepancy import (
    US_MA_RESIDENT_EGT_DISCREPANCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.analyst_data_validation.us_ma_resident_monthly_credit_activity_discrepancy import (
    US_MA_RESIDENT_MONTHLY_CREDIT_ACTIVITY_DISCREPANCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.analyst_data_validation.us_pa.us_pa_releases_preprocessed_validations import (
    US_PA_DIFFERENT_INMATE_ID_AND_INMATE_ID_OLD_VIEW_BUILDER,
    US_PA_IS_SCI_VIEW_BUILDER,
    US_PA_NO_NEEDS_CATEGORY_RELEASE_STATUS_VIEW_BUILDER,
    US_PA_NO_NULL_SPAN_EXTERNAL_ID_VIEW_BUILDER,
    US_PA_NO_UNKNOWN_RELEASE_STATUS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.dataflow_metrics.configured_validations import (
    get_all_dataflow_metrics_validations,
)
from recidiviz.validation.views.state.in_custody_sps_have_associated_ip import (
    IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_admission_after_open_period import (
    INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_admission_person_level_external_comparison import (
    INCARCERATION_ADMISSION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_commitments_subset_of_admissions import (
    INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_lengths_by_demographics_internal_consistency import (
    INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_population_by_admission_reason_internal_consistency import (
    INCARCERATION_POPULATION_BY_ADMISSION_REASON_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_population_by_demographic_internal_comparison import (
    INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_population_by_facility_by_demographics_internal_consistency import (
    INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_population_by_facility_external_comparison import (
    INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency import (
    INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_population_person_level_external_comparison import (
    INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_population_person_level_external_comparison_matching_people_with_custody_level import (
    INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_CUSTODY_LEVEL_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_population_person_level_external_comparison_matching_people_with_facility import (
    INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_FACILITY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_release_person_level_external_comparison import (
    INCARCERATION_RELEASE_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_release_prior_to_admission import (
    INCARCERATION_RELEASE_PRIOR_TO_ADMISSION_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_release_reason_no_date import (
    INCARCERATION_RELEASE_REASON_NO_DATE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_release_reason_no_release_date import (
    INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_releases_by_type_by_period_internal_consistency import (
    INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.insights_primary_users_not_in_state_staff import (
    INSIGHTS_PRIMARY_USERS_NOT_IN_STATE_STAFF_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_admission_reason_and_pfi import (
    INVALID_ADMISSION_REASON_AND_PFI_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_admission_reasons_for_commitments_from_supervision import (
    INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_admission_reasons_for_temporary_custody import (
    INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_admitted_from_supervision_admission_reason import (
    INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_null_pfi_in_metrics import (
    INVALID_NULL_PFI_IN_METRICS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_null_spfi_in_normalized_ips import (
    INVALID_NULL_SPFI_NORMALIZED_IPS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_pfi_for_temporary_custody_admissions import (
    INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_release_reasons_for_temporary_custody import (
    INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.jii_texting_multiple_welcome_messages import (
    JII_TEXTING_MULTIPLE_WELCOME_MESSAGES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.jii_to_text_percent_change_exceeded import (
    JII_TO_TEXT_PERCENT_CHANGE_EXCEEDED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.location_ids_to_names_unique_ids import (
    LOCATION_IDS_TO_NAMES_UNIQUE_IDS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.location_metadata.configured_validations import (
    get_all_location_metadata_validations,
)
from recidiviz.validation.views.state.multiple_supervision_info_for_commitment_admission import (
    MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_VIEW_BUILDER,
)
from recidiviz.validation.views.state.outliers.configured_validations import (
    get_all_outliers_validations,
)
from recidiviz.validation.views.state.overlapping_incarceration_periods import (
    OVERLAPPING_INCARCERATION_PERIODS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.overlapping_supervision_periods import (
    OVERLAPPING_SUPERVISION_PERIODS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.parole_agent_badge_number_changes import (
    PA_BADGE_NUMBER_CHANGES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.primary_keys_unique_across_all_states import (
    PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.product_roster_blocked_30_days import (
    PRODUCT_ROSTER_BLOCKED_30_DAYS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.product_roster_upcoming_blocks import (
    PRODUCT_ROSTER_UPCOMING_BLOCKS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.raw_data.configured_validations import (
    get_all_raw_data_validations,
)
from recidiviz.validation.views.state.recidivism_person_level_external_comparison_matching_people import (
    RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.recidivism_release_cohort_person_level_external_comparison import (
    RECIDIVISM_RELEASE_COHORT_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.revocation_matrix_caseload_admission_history import (
    REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.revocation_matrix_comparison_by_month import (
    REVOCATION_MATRIX_COMPARISON_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.validation.views.state.revocation_matrix_comparison_revocation_cell_vs_caseload import (
    REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER,
)
from recidiviz.validation.views.state.revocation_matrix_comparison_revocations_by_officer import (
    REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_VIEW_BUILDER,
)
from recidiviz.validation.views.state.revocation_matrix_comparison_supervision_population import (
    REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW_BUILDER,
)
from recidiviz.validation.views.state.revocation_matrix_distribution_by_gender_comparison import (
    REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.revocation_matrix_distribution_by_race_comparison import (
    REVOCATION_MATRIX_DISTRIBUTION_BY_RACE_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.segment_events_unknown_product_type import (
    SEGMENT_EVENTS_UNKNOWN_PRODUCT_TYPE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentence_sessions.open_sessions_without_active_sentences import (
    OPEN_SESSIONS_WITHOUT_ACTIVE_SENTENCES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentence_sessions.overlapping_sentence_inferred_group_serving_periods import (
    OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentence_type_by_district_by_demographics_internal_consistency import (
    SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.case_insights_rates_missing_charges import (
    SENTENCES_CASE_INSIGHTS_CHARGE_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.compare_v2_and_v1_sentences import (
    SENTENCE_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.normalized_state_charge_missing_descriptions import (
    NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.normalized_state_charge_missing_uniform_offense_labels import (
    NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.sentence_inferred_group_projected_dates_mismatch import (
    SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VALIDATION_VIEW_BUILDER,
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
from recidiviz.validation.views.state.sentences.session_new_admissions_with_no_sentence_date_imposed import (
    SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentences.sessions_missing_closest_sentence_imposed_group import (
    SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sessions.mismatched_supervision_staff_attribute_sessions_locations import (
    MISMATCHED_SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_LOCATION_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sessions.person_caseload_location_sessions import (
    PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sessions.session_location_names_no_duplicates import (
    SESSION_LOCATION_NAMES_NO_DUPLICATES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sessions.sessions_persons_in_incarceration_or_supervision import (
    SESSIONS_IN_INCARCERATION_OR_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.validation.views.state.stable_counts.configured_validations import (
    get_all_stable_counts_validations,
)
from recidiviz.validation.views.state.supervision_population_by_district_by_demographics_internal_consistency import (
    SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency import (
    SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_population_by_type_external_comparison import (
    SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_population_person_level_external_comparison import (
    SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_population_person_level_external_comparison_matching_people import (
    SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_DISTRICT_VIEW_BUILDER,
    SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_LEVEL_VIEW_BUILDER,
    SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_OFFICER_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_revocations_by_period_by_type_by_demographics_internal_consistency import (
    SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_start_person_level_external_comparison import (
    SUPERVISION_START_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_termination_person_level_external_comparison import (
    SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_termination_prior_to_start import (
    SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_termination_reason_no_date import (
    SUPERVISION_TERMINATION_REASON_NO_DATE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.us_me_invalid_snooze_notes import (
    US_ME_INVALID_SNOOZE_NOTES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.user_metrics.officer_monthly_usage_report_actions_without_logins import (
    OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.user_metrics.officer_monthly_usage_report_duplicate_rows import (
    OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.user_metrics.officer_monthly_usage_report_vs_impact_report_active_users_supervision import (
    OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.validation.views.state.user_metrics.officer_monthly_usage_report_vs_impact_report_registered_users_supervision import (
    OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_REGISTERED_USERS_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.client_and_resident_record_percent_change_in_eligibility_exceeded import (
    CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.client_record_archive_duplicate_person_ids import (
    CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.client_record_duplicate_person_external_ids import (
    CLIENT_RECORD_DUPLICATE_PERSON_EXTERNAL_IDS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.missing_client_record_rows_null_full_name import (
    MISSING_CLIENT_RECORD_ROWS_NULL_FULL_NAME_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.missing_client_record_rows_null_officer import (
    MISSING_CLIENT_RECORD_ROWS_NULL_OFFICER_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.missing_client_record_rows_unknown_reason import (
    MISSING_CLIENT_RECORD_ROWS_UNKNOWN_REASON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.opportunites_without_person_records import (
    OPPORTUNITIES_WITHOUT_PERSON_RECORDS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.pseudonymized_id_to_person_id_missing_ids import (
    PSEUDONYMIZED_ID_TO_PERSON_ID_MISSING_IDS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.pseudonymized_id_to_person_id_valid_primary_key import (
    PSEUDONYMIZED_ID_TO_PERSON_ID_VALID_PRIMARY_KEY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows.us_mi_flag_new_offense_codes import (
    US_MI_FLAG_NEW_OFFENSE_CODES_VIEW_BUILDER,
)
from recidiviz.validation.views.state.workflows_primary_users_not_in_state_staff import (
    WORKFLOWS_PRIMARY_USERS_NOT_IN_STATE_STAFF_VIEW_BUILDER,
)
from recidiviz.validation.views.static_reference_tables.experiment_assignments_unit_of_analysis_validation import (
    EXPERIMENT_ASSIGNMENTS_UNIT_OF_ANALYSIS_VALIDATION_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.completion_event_type_mismatch import (
    COMPLETION_EVENT_TYPE_MISMATCH_VIEW_BUILDER,
)
from recidiviz.validation.views.task_eligibility.configured_validations import (
    get_all_task_eligibility_validations,
)


def _get_validation_region_module_paths() -> List[Tuple[str, str]]:
    region_modules = ModuleCollectorMixin.get_submodules(
        validation_regions, submodule_name_prefix_filter=None
    )
    validation_region_module_paths = []
    for region_module in region_modules:
        if region_module.__file__ is None:
            raise ValueError(f"No file associated with {region_module}.")
        region_module_path = os.path.dirname(region_module.__file__)
        region_code = os.path.split(region_module_path)[-1].lower()
        validation_region_module_paths.append((region_code, region_module_path))

    return validation_region_module_paths


@cache
def get_validation_region_configs() -> Dict[str, ValidationRegionConfig]:
    """Reads all region configs for regions with configs defined in the
    recidiviz.validation.config.regions module.
    """

    validation_region_configs = {}
    for region_code, region_module_path in _get_validation_region_module_paths():
        config_path = os.path.join(
            region_module_path, f"{region_code.lower()}_validation_config.yaml"
        )
        validation_region_configs[
            region_code.upper()
        ] = ValidationRegionConfig.from_yaml(config_path)

    return validation_region_configs


def get_validation_global_config() -> ValidationGlobalConfig:
    return ValidationGlobalConfig.from_yaml(
        os.path.join(os.path.dirname(config.__file__), "validation_global_config.yaml")
    )


@cache
def get_all_validations() -> List[DataValidationCheck]:
    """Returns the full list of configured validations to perform. This is not built as a top-level variable because the
    views cannot be built locally being run inside of a local_project_id_override block.
    """

    region_configs = get_validation_region_configs()

    all_data_validations: List[DataValidationCheck] = [
        *get_all_task_eligibility_validations(),
        *get_all_dataflow_metrics_validations(),
        *get_all_raw_data_validations(),
        *get_all_location_metadata_validations(),
        *get_all_stable_counts_validations(region_configs),
        *get_all_outliers_validations(region_configs),
        ExistenceDataValidationCheck(
            view_builder=ADMISSION_PFI_POP_PFI_MISMATCH_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INCARCERATION_RELEASE_PRIOR_TO_ADMISSION_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INCARCERATION_RELEASE_REASON_NO_DATE_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=OVERLAPPING_INCARCERATION_PERIODS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            IN_CUSTODY_SPS_HAVE_ASSOCIATED_IP_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=US_MI_FLAG_NEW_OFFENSE_CODES_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SUPERVISION_TERMINATION_REASON_NO_DATE_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=OVERLAPPING_SUPERVISION_PERIODS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SESSIONS_IN_INCARCERATION_OR_SUPERVISION_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_ADMISSION_REASONS_FOR_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_NULL_PFI_IN_METRICS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        # TODO(#3275): Delete this view once all specialized_purpose_for_incarceration
        #  metric fields have been re-named to purpose_for_incarceration
        ExistenceDataValidationCheck(
            view_builder=INVALID_NULL_SPFI_NORMALIZED_IPS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_RELEASE_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_ADMISSION_REASON_AND_PFI_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=OVERLAPPING_SENTENCE_INFERRED_GROUP_SERVING_PERIODS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SESSION_NEW_ADMISSIONS_WITH_NO_SENTENCE_DATE_IMPOSED_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SESSION_LIBERTY_RELEASES_WITH_NO_SENTENCE_COMPLETION_DATE_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SENTENCES_MISSING_DATE_IMPOSED_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SENTENCES_UNDEFINED_RELATIONSHIP_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SENTENCES_CASE_INSIGHTS_CHARGE_COMPARISON_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
            hard_num_allowed_rows=5,
        ),
        ExistenceDataValidationCheck(
            view_builder=LOCATION_IDS_TO_NAMES_UNIQUE_IDS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=OPPORTUNITIES_WITHOUT_PERSON_RECORDS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=NORMALIZED_STATE_CHARGE_MISSING_DESCRIPTIONS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=NORMALIZED_STATE_CHARGE_MISSING_UNIFORM_OFFENSE_LABELS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SESSIONS_MISSING_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=EXPERIMENT_ASSIGNMENTS_UNIT_OF_ANALYSIS_VALIDATION_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=PA_BADGE_NUMBER_CHANGES_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SESSION_LOCATION_NAMES_NO_DUPLICATES_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=COMPLETION_EVENT_TYPE_MISMATCH_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INSIGHTS_PRIMARY_USERS_NOT_IN_STATE_STAFF_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
            projects_to_deploy={GCP_PROJECT_PRODUCTION},
        ),
        ExistenceDataValidationCheck(
            view_builder=WORKFLOWS_PRIMARY_USERS_NOT_IN_STATE_STAFF_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
            projects_to_deploy={GCP_PROJECT_PRODUCTION},
        ),
        ExistenceDataValidationCheck(
            view_builder=US_ME_INVALID_SNOOZE_NOTES_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=OFFICER_MONTHLY_USAGE_REPORT_ACTIONS_WITHOUT_LOGINS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=OFFICER_MONTHLY_USAGE_REPORT_DUPLICATE_ROWS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=PRODUCT_ROSTER_UPCOMING_BLOCKS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
            projects_to_deploy={GCP_PROJECT_PRODUCTION},
        ),
        ExistenceDataValidationCheck(
            view_builder=PRODUCT_ROSTER_BLOCKED_30_DAYS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
            projects_to_deploy={GCP_PROJECT_PRODUCTION},
        ),
        ExistenceDataValidationCheck(
            view_builder=US_MA_RESIDENT_EGT_DISCREPANCY_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=US_MA_RESIDENT_MONTHLY_CREDIT_ACTIVITY_DISCREPANCY_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=US_PA_NO_NULL_SPAN_EXTERNAL_ID_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=US_PA_NO_NEEDS_CATEGORY_RELEASE_STATUS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
            hard_num_allowed_rows=45,
        ),
        ExistenceDataValidationCheck(
            view_builder=US_PA_NO_UNKNOWN_RELEASE_STATUS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
            hard_num_allowed_rows=100,
        ),
        ExistenceDataValidationCheck(
            view_builder=US_PA_DIFFERENT_INMATE_ID_AND_INMATE_ID_OLD_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
            hard_num_allowed_rows=300,
        ),
        ExistenceDataValidationCheck(
            view_builder=US_PA_IS_SCI_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_VIEW_BUILDER,
            comparison_columns=[
                "total_revocation_admissions",
                "total_caseload_admissions",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER,
            comparison_columns=["cell_sum", "caseload_sum", "caseload_num_rows"],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_COMPARISON_BY_MONTH_VIEW_BUILDER,
            comparison_columns=["reference_sum", "month_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW_BUILDER,
            comparison_columns=[
                "district_sum",
                "risk_level_sum",
                "gender_sum",
                "race_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_VIEW_BUILDER,
            comparison_columns=["officer_sum", "caseload_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_RACE_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="revocation",
            comparison_columns=["revocation_count_all", "revocation_count_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_RACE_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="supervision",
            comparison_columns=[
                "supervision_count_all",
                "supervision_population_count_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_RACE_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="recommendation",
            comparison_columns=[
                "recommended_for_revocation_count_all",
                "recommended_for_revocation_count_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="revocation",
            comparison_columns=["revocation_count_all", "revocation_count_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="supervision",
            comparison_columns=[
                "supervision_count_all",
                "supervision_population_count_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="recommendation",
            comparison_columns=[
                "recommended_for_revocation_count_all",
                "recommended_for_revocation_count_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_BUILDER,
            comparison_columns=[
                "population_by_admission_reason_total_population",
                "population_by_facility_by_demographics_total_population",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_ADMISSION_REASON_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=[
                "metric_total",
                "age_bucket_breakdown_sum",
                "race_or_ethnicity_breakdown_sum",
                "gender_breakdown_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=[
                "metric_total",
                "age_bucket_breakdown_sum",
                "race_or_ethnicity_breakdown_sum",
                "gender_breakdown_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=["metric_total", "race_or_ethnicity_breakdown_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=["metric_total", "race_or_ethnicity_breakdown_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=[
                "metric_total",
                "age_bucket_breakdown_sum",
                "race_or_ethnicity_breakdown_sum",
                "gender_breakdown_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=[
                "metric_total",
                "age_bucket_breakdown_sum",
                "race_or_ethnicity_breakdown_sum",
                "gender_breakdown_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=[
                "metric_total",
                "age_bucket_breakdown_sum",
                "race_or_ethnicity_breakdown_sum",
                "gender_breakdown_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=[
                "metric_total",
                "age_bucket_breakdown_sum",
                "race_or_ethnicity_breakdown_sum",
                "gender_breakdown_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=[
                "metric_total",
                "age_bucket_breakdown_sum",
                "race_or_ethnicity_breakdown_sum",
                "gender_breakdown_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=["metric_total", "race_or_ethnicity_breakdown_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_VIEW_BUILDER,
            comparison_columns=[
                "unique_person_ids",
                "client_records",
            ],
            soft_max_allowed_error=0.0,
            hard_max_allowed_error=0.0,
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=CLIENT_RECORD_DUPLICATE_PERSON_EXTERNAL_IDS_VIEW_BUILDER,
            comparison_columns=[
                "unique_person_external_ids",
                "client_records",
            ],
            soft_max_allowed_error=0.0,
            hard_max_allowed_error=0.0,
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        ExistenceDataValidationCheck(
            view_builder=PSEUDONYMIZED_ID_TO_PERSON_ID_MISSING_IDS_VIEW_BUILDER,
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        ExistenceDataValidationCheck(
            view_builder=PSEUDONYMIZED_ID_TO_PERSON_ID_VALID_PRIMARY_KEY_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        # External comparison validations
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_ADMISSION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_data_person_id",
                "internal_data_person_id",
            ],
            partition_columns=["region_code", "admission_date"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_BUILDER,
            comparison_columns=[
                "external_population_count",
                "internal_population_count",
            ],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_VIEW_BUILDER,
            comparison_columns=[
                "external_population_count",
                "internal_population_count",
            ],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_data_person_id",
                "internal_data_person_id",
            ],
            partition_columns=["region_code", "date_of_stay"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_FACILITY_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=["external_facility", "internal_facility"],
            partition_columns=["region_code", "date_of_stay"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_CUSTODY_LEVEL_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=["external_custody_level", "internal_custody_level"],
            partition_columns=["region_code", "date_of_stay"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_RELEASE_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_data_person_id",
                "internal_data_person_id",
            ],
            partition_columns=["region_code", "release_date"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_person_id",
                "internal_person_id",
            ],
            partition_columns=["region_code", "date_of_supervision"],
            hard_max_allowed_error=0.2,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_DISTRICT_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=["external_district", "internal_district"],
            partition_columns=["region_code", "date_of_supervision"],
            hard_max_allowed_error=0.01,
            soft_max_allowed_error=0.01,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_LEVEL_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_supervision_level",
                "internal_supervision_level",
            ],
            partition_columns=["region_code", "date_of_supervision"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_WITH_OFFICER_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_supervising_officer",
                "internal_supervising_officer",
            ],
            partition_columns=["region_code", "date_of_supervision"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=RECIDIVISM_RELEASE_COHORT_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_person_id",
                "internal_person_id",
            ],
            partition_columns=["region_code", "release_cohort", "follow_up_period"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=["external_recidivated", "internal_recidivated"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_START_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_person_id",
                "internal_person_id",
            ],
            partition_columns=["region_code", "start_date"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "external_person_id",
                "internal_person_id",
            ],
            partition_columns=["region_code", "termination_date"],
            hard_max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_VIEW_BUILDER,
            comparison_columns=[
                "prev_eligibility_count",
                "current_eligibility_count",
            ],
            soft_max_allowed_error=0.10,
            hard_max_allowed_error=0.30,
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=JII_TO_TEXT_PERCENT_CHANGE_EXCEEDED_VIEW_BUILDER,
            comparison_columns=[
                "prev_jii_count",
                "current_jii_count",
            ],
            soft_max_allowed_error=0.10,
            hard_max_allowed_error=0.30,
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        ExistenceDataValidationCheck(
            view_builder=JII_TEXTING_MULTIPLE_WELCOME_MESSAGES_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        SamenessDataValidationCheck(
            view_builder=PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            comparison_columns=[
                "total_count",
                "distinct_id_count",
            ],
            validation_category=ValidationCategory.INVARIANT,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=SENTENCE_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "charge_v1_external_id",
                "charge_v2_external_id",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER,
            validation_name_suffix="caseload",
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "caseload_id",
                "officer_id",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER,
            validation_name_suffix="location",
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "location_detail_id",
                "location",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
        ),
        SamenessDataValidationCheck(
            view_builder=OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_ACTIVE_USERS_SUPERVISION_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            comparison_columns=[
                "officer_monthly_usage_report_active_users_supervision",
                "impact_report_active_users_supervision",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
            hard_max_allowed_error=0.05,
            projects_to_deploy={GCP_PROJECT_PRODUCTION},
        ),
        SamenessDataValidationCheck(
            view_builder=OFFICER_MONTHLY_USAGE_REPORT_VS_IMPACT_REPORT_REGISTERED_USERS_SUPERVISION_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_ROW,
            comparison_columns=[
                "officer_monthly_usage_report_registered_users_supervision",
                "impact_report_registered_users_supervision",
                "impact_report_registered_users_supervision_by_district",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
            hard_max_allowed_error=0.05,
            projects_to_deploy={GCP_PROJECT_PRODUCTION},
        ),
        SamenessDataValidationCheck(
            view_builder=MISMATCHED_SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_LOCATION_VIEW_BUILDER,
            validation_name_suffix="district",
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "supervision_district_id",
                "supervision_district_id_inferred",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
            soft_max_allowed_error=0.3,
            hard_max_allowed_error=0.5,
        ),
        SamenessDataValidationCheck(
            view_builder=MISMATCHED_SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_LOCATION_VIEW_BUILDER,
            validation_name_suffix="office",
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "supervision_office_id",
                "supervision_office_id_inferred",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
            region_configs=region_configs,
            soft_max_allowed_error=0.3,
            hard_max_allowed_error=0.5,
        ),
        ExistenceDataValidationCheck(
            view_builder=SENTENCE_INFERRED_GROUP_PROJECTED_DATES_VALIDATION_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        SamenessDataValidationCheck(
            view_builder=OPEN_SESSIONS_WITHOUT_ACTIVE_SENTENCES_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.PER_VIEW,
            comparison_columns=[
                "has_open_session",
                "has_active_sentence",
            ],
            validation_category=ValidationCategory.INVARIANT,
            region_configs=region_configs,
        ),
        ExistenceDataValidationCheck(
            view_builder=MISSING_CLIENT_RECORD_ROWS_NULL_FULL_NAME_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=MISSING_CLIENT_RECORD_ROWS_NULL_OFFICER_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=MISSING_CLIENT_RECORD_ROWS_UNKNOWN_REASON_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SEGMENT_EVENTS_UNKNOWN_PRODUCT_TYPE_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
    ]

    return all_data_validations


def get_all_deployed_validations() -> List[DataValidationCheck]:
    """Returns the full list of configured validations to perform for the current
    project_id.
    """
    return [
        validation
        for validation in get_all_validations()
        if validation.should_deploy_in_project(metadata.project_id())
    ]


def get_all_deployed_validations_by_name() -> Dict[str, DataValidationCheck]:
    return {v.validation_name: v for v in get_all_deployed_validations()}
