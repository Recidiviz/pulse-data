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
from typing import Dict, List, Tuple

from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.utils import regions
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
from recidiviz.validation.views.case_triage.assessment_freshness_validation import (
    ASSESSMENT_FRESHNESS_VALIDATION_VIEW_BUILDER,
)
from recidiviz.validation.views.case_triage.contact_freshness_validation import (
    CONTACT_FRESHNESS_VALIDATION_VIEW_BUILDER,
)
from recidiviz.validation.views.case_triage.employment_freshness_validation import (
    EMPLOYMENT_FRESHNESS_VALIDATION_VIEW_BUILDER,
)
from recidiviz.validation.views.case_triage.etl_freshness_validation import (
    ETL_FRESHNESS_VALIDATION_VIEW_BUILDER,
)
from recidiviz.validation.views.justice_counts.incarceration_population_by_state_by_date_justice_counts_comparison import (
    INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER,
)

# pylint: disable=line-too-long
from recidiviz.validation.views.state.active_in_population_after_death_date import (
    ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.active_program_participation_by_region_internal_consistency import (
    ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.case_termination_by_type_comparison import (
    CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.ftr_referrals_comparison import (
    FTR_REFERRALS_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_admission_after_open_period import (
    INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_admission_nulls import (
    INCARCERATION_ADMISSION_NULLS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.incarceration_admission_person_level_external_comparison import (
    INCARCERATION_ADMISSION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
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
from recidiviz.validation.views.state.incarceration_population_person_level_external_comparison_matching_people import (
    INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
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
from recidiviz.validation.views.state.invalid_admission_reasons_for_temporary_custody import (
    INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_admitted_from_supervision_admission_reason import (
    INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.invalid_pfi_for_temporary_custody_admissions import (
    INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.overlapping_incarceration_periods import (
    OVERLAPPING_INCARCERATION_PERIODS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.overlapping_supervision_periods import (
    OVERLAPPING_SUPERVISION_PERIODS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.po_report_avgs_per_district_state import (
    PO_REPORT_AVGS_PER_DISTRICT_STATE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.po_report_distinct_by_officer_month import (
    PO_REPORT_DISTINCT_BY_OFFICER_MONTH_VIEW_BUILDER,
)
from recidiviz.validation.views.state.po_report_missing_fields import (
    PO_REPORT_COMPARISON_COLUMNS,
    PO_REPORT_MISSING_FIELDS_VIEW_BUILDER,
)
from recidiviz.validation.views.state.population_projection_data_validation.county_jail_population_person_level_external_comparison import (
    COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
    COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.population_projection_data_validation.population_projection_monthly_population_external_comparison import (
    POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_VIEW_BUILDER,
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
from recidiviz.validation.views.state.revocation_matrix_comparison_revocation_cell_vs_month import (
    REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW_BUILDER,
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
from recidiviz.validation.views.state.revocations_by_period_dashboard_comparison import (
    REVOCATIONS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.sentence_type_by_district_by_demographics_internal_consistency import (
    SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_population_by_district_by_demographics_internal_consistency import (
    SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_population_by_prioritized_race_and_ethnicity_by_period_internal_consistency import (
    SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_population_person_level_external_comparison import (
    SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_population_person_level_external_comparison_matching_people import (
    SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_revocations_by_period_by_type_by_demographics_internal_consistency import (
    SUPERVISION_REVOCATIONS_BY_PERIOD_BY_TYPE_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_start_person_level_external_comparison import (
    SUPERVISION_START_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_success_by_month_dashboard_comparison import (
    SUPERVISION_SUCCESS_BY_MONTH_DASHBOARD_COMPARISON_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_success_by_period_by_demographics_internal_consistency import (
    SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
)
from recidiviz.validation.views.state.supervision_success_by_period_dashboard_comparison import (
    SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER,
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


def _get_validation_region_module_paths() -> List[Tuple[str, str]]:
    region_modules = ModuleCollectorMixin.get_submodules(
        validation_regions, submodule_name_prefix_filter=None
    )
    validation_region_module_paths = []
    for region_module in region_modules:
        region_module_path = os.path.dirname(region_module.__file__)
        region_code = os.path.split(region_module_path)[-1].lower()
        validation_region_module_paths.append((region_code, region_module_path))

    return validation_region_module_paths


def get_validation_region_configs() -> Dict[str, ValidationRegionConfig]:
    """Reads all region configs for regions with configs defined in the recidiviz.validation.config.regions module. This
    is the set of regions we will run validations for, subject to the constraints defined in their validation config
    files.
    """

    validation_region_configs = {}
    for region_code, region_module_path in _get_validation_region_module_paths():
        region = regions.get_region(region_code.lower(), is_direct_ingest=True)
        if region.is_ingest_launched_in_env():
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


def get_all_validations() -> List[DataValidationCheck]:
    """Returns the full list of configured validations to perform. This is not built as a top-level variable because the
    views cannot be built locally being run inside of a local_project_id_override block.
    """

    all_data_validations: List[DataValidationCheck] = [
        ExistenceDataValidationCheck(
            view_builder=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INCARCERATION_ADMISSION_NULLS_VIEW_BUILDER,
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
            view_builder=PO_REPORT_AVGS_PER_DISTRICT_STATE_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=PO_REPORT_DISTINCT_BY_OFFICER_MONTH_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW_BUILDER,
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
            view_builder=INVALID_ADMISSION_REASONS_FOR_TEMPORARY_CUSTODY_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_ADMITTED_FROM_SUPERVISION_ADMISSION_REASON_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=INVALID_PFI_FOR_TEMPORARY_CUSTODY_ADMISSIONS_VIEW_BUILDER,
            validation_category=ValidationCategory.INVARIANT,
        ),
        ExistenceDataValidationCheck(
            view_builder=ASSESSMENT_FRESHNESS_VALIDATION_VIEW_BUILDER,
            validation_category=ValidationCategory.FRESHNESS,
        ),
        ExistenceDataValidationCheck(
            view_builder=CONTACT_FRESHNESS_VALIDATION_VIEW_BUILDER,
            validation_category=ValidationCategory.FRESHNESS,
        ),
        ExistenceDataValidationCheck(
            view_builder=EMPLOYMENT_FRESHNESS_VALIDATION_VIEW_BUILDER,
            validation_category=ValidationCategory.FRESHNESS,
        ),
        ExistenceDataValidationCheck(
            view_builder=ETL_FRESHNESS_VALIDATION_VIEW_BUILDER,
            validation_category=ValidationCategory.FRESHNESS,
        ),
        SamenessDataValidationCheck(
            view_builder=CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="absconsions",
            comparison_columns=["absconsions_by_month", "absconsions_from_po_report"],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="discharges",
            comparison_columns=["discharges_by_month", "discharges_from_po_report"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=FTR_REFERRALS_COMPARISON_VIEW_BUILDER,
            comparison_columns=[
                "age_bucket_sum",
                "risk_level_sum",
                "gender_sum",
                "race_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=PO_REPORT_MISSING_FIELDS_VIEW_BUILDER,
            comparison_columns=PO_REPORT_COMPARISON_COLUMNS,
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_CASELOAD_ADMISSION_HISTORY_VIEW_BUILDER,
            comparison_columns=[
                "total_revocation_admissions",
                "total_caseload_admissions",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER,
            comparison_columns=["cell_sum", "caseload_sum", "caseload_num_rows"],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW_BUILDER,
            comparison_columns=["cell_sum", "month_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_COMPARISON_BY_MONTH_VIEW_BUILDER,
            comparison_columns=["reference_sum", "month_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
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
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_COMPARISON_REVOCATIONS_BY_OFFICER_VIEW_BUILDER,
            comparison_columns=["officer_sum", "caseload_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_RACE_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="revocation",
            comparison_columns=["revocation_count_all", "revocation_count_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_RACE_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="supervision",
            comparison_columns=[
                "supervision_count_all",
                "supervision_population_count_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_RACE_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="recommendation",
            comparison_columns=[
                "recommended_for_revocation_count_all",
                "recommended_for_revocation_count_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="revocation",
            comparison_columns=["revocation_count_all", "revocation_count_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="supervision",
            comparison_columns=[
                "supervision_count_all",
                "supervision_population_count_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATION_MATRIX_DISTRIBUTION_BY_GENDER_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="recommendation",
            comparison_columns=[
                "recommended_for_revocation_count_all",
                "recommended_for_revocation_count_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=REVOCATIONS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER,
            comparison_columns=[
                "dashboard_revocation_count",
                "public_dashboard_revocation_count",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_SUCCESS_BY_MONTH_DASHBOARD_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="termination",
            comparison_columns=[
                "dashboard_successful_termination",
                "public_dashboard_successful_termination",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_SUCCESS_BY_MONTH_DASHBOARD_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="completion",
            comparison_columns=[
                "dashboard_projected_completion",
                "public_dashboard_projected_completion",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="termination",
            comparison_columns=[
                "dashboard_successful_termination",
                "public_dashboard_successful_termination",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER,
            validation_name_suffix="completion",
            comparison_columns=[
                "dashboard_projected_completion",
                "public_dashboard_projected_completion",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_BUILDER,
            comparison_columns=[
                "population_by_admission_reason_total_population",
                "population_by_facility_by_demographics_total_population",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
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
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=["metric_total", "race_or_ethnicity_breakdown_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=["metric_total", "race_or_ethnicity_breakdown_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
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
        ),
        # TODO(#3743): This validation will fail until we fix the view to handle people who age into new buckets
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=[
                "metric_total",
                "race_or_ethnicity_breakdown_sum",
                "gender_breakdown_sum",
            ],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        SamenessDataValidationCheck(
            view_builder=ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_INTERNAL_CONSISTENCY_VIEW_BUILDER,
            comparison_columns=["metric_total", "race_or_ethnicity_breakdown_sum"],
            validation_category=ValidationCategory.CONSISTENCY,
        ),
        # External comparison validations
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_ADMISSION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_data_person_id",
                "internal_data_person_id",
            ],
            partition_columns=["region_code", "admission_date"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_BUILDER,
            comparison_columns=[
                "external_population_count",
                "internal_population_count",
            ],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_data_person_id",
                "internal_data_person_id",
            ],
            partition_columns=["region_code", "date_of_stay"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
            validation_name_suffix="facility",
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=["external_facility", "internal_facility"],
            partition_columns=["region_code", "date_of_stay"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_RELEASE_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_data_person_id",
                "internal_data_person_id",
            ],
            partition_columns=["region_code", "release_date"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_person_external_id",
                "internal_person_external_id",
            ],
            partition_columns=["region_code", "date_of_supervision"],
            max_allowed_error=0.2,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
            validation_name_suffix="district",
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=["external_district", "internal_district"],
            partition_columns=["region_code", "date_of_supervision"],
            max_allowed_error=0.01,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
            validation_name_suffix="supervision_level",
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_supervision_level",
                "internal_supervision_level",
            ],
            partition_columns=["region_code", "date_of_supervision"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
            validation_name_suffix="supervising_officer",
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_supervising_officer",
                "internal_supervising_officer",
            ],
            partition_columns=["region_code", "date_of_supervision"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=RECIDIVISM_RELEASE_COHORT_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_person_external_id",
                "internal_person_external_id",
            ],
            partition_columns=["region_code", "release_cohort", "follow_up_period"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=["external_recidivated", "internal_recidivated"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_START_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_person_external_id",
                "internal_person_external_id",
            ],
            partition_columns=["region_code", "start_date"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=SUPERVISION_TERMINATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_person_external_id",
                "internal_person_external_id",
            ],
            partition_columns=["region_code", "termination_date"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=[
                "external_person_external_id",
                "internal_person_external_id",
            ],
            partition_columns=["region_code", "date_of_stay"],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
            validation_name_suffix="facility",
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=["external_facility", "internal_facility"],
            partition_columns=["region_code", "date_of_stay"],
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER,
            validation_name_suffix="legal_status",
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=["external_legal_status", "internal_legal_status"],
            partition_columns=["region_code", "date_of_stay"],
            validation_category=ValidationCategory.EXTERNAL_INDIVIDUAL,
        ),
        SamenessDataValidationCheck(
            view_builder=POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=[
                "external_total_population",
                "internal_total_population",
            ],
            max_allowed_error=0.02,
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
        ),
        SamenessDataValidationCheck(
            view_builder=INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER,
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=[
                "justice_counts_total_population",
                "internal_total_population",
            ],
            max_allowed_error=0.06,
            validation_category=ValidationCategory.EXTERNAL_AGGREGATE,
        ),
    ]

    return all_data_validations
