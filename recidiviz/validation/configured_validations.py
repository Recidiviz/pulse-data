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
from typing import List

from recidiviz.validation.checks.existence_check import ExistenceDataValidationCheck
from recidiviz.validation.checks.sameness_check import SamenessDataValidationCheck, SamenessDataValidationCheckType
from recidiviz.validation.validation_models import DataValidationCheck
from recidiviz.validation.views.state.case_termination_by_type_comparison import \
    CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.ftr_referrals_comparison import FTR_REFERRALS_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.incarceration_admission_after_open_period import \
    INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_BUILDER
from recidiviz.validation.views.state.incarceration_admission_nulls import INCARCERATION_ADMISSION_NULLS_VIEW_BUILDER
from recidiviz.validation.views.state.incarceration_population_by_facility_external_comparison import \
    INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.incarceration_population_by_facility_internal_comparison import \
    INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.incarceration_release_prior_to_admission import \
    INCARCERATION_RELEASE_PRIOR_TO_ADMISSION_VIEW_BUILDER
from recidiviz.validation.views.state.incarceration_release_reason_no_release_date import \
    INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_VIEW_BUILDER
from recidiviz.validation.views.state.po_report_avgs_per_district_state import \
    PO_REPORT_AVGS_PER_DISTRICT_STATE_VIEW_BUILDER
from recidiviz.validation.views.state.po_report_distinct_by_officer_month import \
    PO_REPORT_DISTINCT_BY_OFFICER_MONTH_VIEW_BUILDER
from recidiviz.validation.views.state.po_report_missing_fields import PO_REPORT_MISSING_FIELDS_VIEW_BUILDER, \
    PO_REPORT_COMPARISON_COLUMNS
from recidiviz.validation.views.state.revocation_matrix_comparison_revocation_cell_vs_caseload import \
    REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER
from recidiviz.validation.views.state.revocation_matrix_comparison_revocation_cell_vs_month import \
    REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW_BUILDER
from recidiviz.validation.views.state.revocation_matrix_comparison_supervision_population import \
    REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW_BUILDER
from recidiviz.validation.views.state.revocations_by_period_by_race_or_ethnicity_dashboard_comparison import \
    REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.revocations_by_violation_type_dashboard_comparison import \
    REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.supervision_eom_population_person_level_district_external_comparison import \
    SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.supervision_population_by_district_dashboard_comparison import \
    SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.supervision_success_by_month_dashboard_comparison import \
    SUPERVISION_SUCCESS_BY_MONTH_DASHBOARD_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.supervision_success_by_period_dashboard_comparison import \
    SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER
from recidiviz.validation.views.state.supervision_termination_prior_to_start import \
    SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW_BUILDER

STATES_TO_VALIDATE = ['US_ID', 'US_MO', 'US_ND', 'US_PA']


def get_all_validations() -> List[DataValidationCheck]:
    """Returns the full list of configured validations to perform. This is not built as a top-level variable because the
     views cannot be built locally being run inside of a local_project_id_override block.
     """

    all_data_validations: List[DataValidationCheck] = [
        ExistenceDataValidationCheck(view=INCARCERATION_ADMISSION_AFTER_OPEN_PERIOD_VIEW_BUILDER.build()),
        ExistenceDataValidationCheck(view=INCARCERATION_ADMISSION_NULLS_VIEW_BUILDER.build()),
        ExistenceDataValidationCheck(view=INCARCERATION_RELEASE_PRIOR_TO_ADMISSION_VIEW_BUILDER.build()),

        # TODO(2981): This should stop failing for MO once we fix the 600ish periods with end dates of 99999999
        ExistenceDataValidationCheck(view=INCARCERATION_RELEASE_REASON_NO_RELEASE_DATE_VIEW_BUILDER.build()),

        ExistenceDataValidationCheck(view=PO_REPORT_AVGS_PER_DISTRICT_STATE_VIEW_BUILDER.build()),
        ExistenceDataValidationCheck(view=PO_REPORT_DISTINCT_BY_OFFICER_MONTH_VIEW_BUILDER.build()),
        ExistenceDataValidationCheck(view=SUPERVISION_TERMINATION_PRIOR_TO_START_VIEW_BUILDER.build()),

        SamenessDataValidationCheck(view=CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER.build(),
                                    comparison_columns=['absconsions_by_month', 'absconsions_by_officer'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS),
        SamenessDataValidationCheck(view=CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER.build(),
                                    comparison_columns=['discharges_by_month', 'discharges_by_officer'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    max_allowed_error=0.02),
        SamenessDataValidationCheck(view=FTR_REFERRALS_COMPARISON_VIEW_BUILDER.build(),
                                    comparison_columns=['age_bucket_sum', 'risk_level_sum', 'gender_sum', 'race_sum'],
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    max_allowed_error=0.06),
        SamenessDataValidationCheck(view=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_BUILDER.build(),
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    comparison_columns=['external_population_count', 'internal_population_count']),
        SamenessDataValidationCheck(view=PO_REPORT_MISSING_FIELDS_VIEW_BUILDER.build(),
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    comparison_columns=PO_REPORT_COMPARISON_COLUMNS),
        SamenessDataValidationCheck(view=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_CASELOAD_VIEW_BUILDER.build(),
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    comparison_columns=['cell_sum', 'caseload_sum']),
        SamenessDataValidationCheck(view=REVOCATION_MATRIX_COMPARISON_REVOCATION_CELL_VS_MONTH_VIEW_BUILDER.build(),
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    comparison_columns=['cell_sum', 'month_sum'],
                                    max_allowed_error=0.03),
        SamenessDataValidationCheck(view=REVOCATION_MATRIX_COMPARISON_SUPERVISION_POPULATION_VIEW_BUILDER.build(),
                                    sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
                                    comparison_columns=['district_sum', 'risk_level_sum', 'gender_sum', 'race_sum']),
        SamenessDataValidationCheck(
            view=SUPERVISION_EOM_POPULATION_PERSON_LEVEL_DISTRICT_EXTERNAL_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.STRINGS,
            comparison_columns=['external_district', 'internal_district'],
            max_allowed_error=0.01),
        SamenessDataValidationCheck(
            view=REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_absconsion_count', 'public_dashboard_absconsion_count']
        ),
        SamenessDataValidationCheck(
            view=REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_new_crime_count', 'public_dashboard_new_crime_count']
        ),
        SamenessDataValidationCheck(
            view=REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_technical_count', 'public_dashboard_technical_count']
        ),
        SamenessDataValidationCheck(
            view=REVOCATIONS_BY_VIOLATION_TYPE_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_unknown_count', 'public_dashboard_unknown_count']
        ),
        SamenessDataValidationCheck(
            view=SUPERVISION_POPULATION_BY_DISTRICT_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_supervision_count', 'public_dashboard_supervision_count']
        ),
        SamenessDataValidationCheck(
            view=SUPERVISION_SUCCESS_BY_MONTH_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_successful_termination', 'public_dashboard_successful_termination']
        ),
        SamenessDataValidationCheck(
            view=SUPERVISION_SUCCESS_BY_MONTH_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_projected_completion', 'public_dashboard_projected_completion']
        ),
        SamenessDataValidationCheck(
            view=SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_successful_termination', 'public_dashboard_successful_termination']
        ),
        SamenessDataValidationCheck(
            view=SUPERVISION_SUCCESS_BY_PERIOD_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_projected_completion', 'public_dashboard_projected_completion']
        ),
        SamenessDataValidationCheck(
            view=INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['covid_report_facility_population', 'public_dashboard_facility_population']
        ),
        SamenessDataValidationCheck(
            view=REVOCATIONS_BY_PERIOD_BY_RACE_OR_ETHNICITY_DASHBOARD_COMPARISON_VIEW_BUILDER.build(),
            sameness_check_type=SamenessDataValidationCheckType.NUMBERS,
            comparison_columns=['dashboard_revocation_count', 'public_dashboard_revocation_count']
        ),
    ]

    return all_data_validations
