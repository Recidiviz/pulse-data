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
"""Defines all Justice Counts metrics for the Prosecution."""

from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.dimensions.common import (
    CaseSeverityType,
    DispositionType,
    ExpenseType,
)
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    CensusRace,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseDeclinedSeverityType,
    DivertedCaseSeverityType,
    FundingType,
    ProsecutedCaseSeverityType,
    ReferredCaseSeverityType,
    StaffType,
)
from recidiviz.justice_counts.includes_excludes.common import (
    CaseloadNumeratorIncludesExcludes,
    CasesDismissedIncludesExcludes,
    CasesResolvedAtTrialIncludesExcludes,
    CasesResolvedByPleaIncludesExcludes,
    FacilitiesAndEquipmentExpensesIncludesExcludes,
    FelonyCaseloadNumeratorIncludesExcludes,
    FelonyCasesIncludesExcludes,
    MisdemeanorCaseloadNumeratorIncludesExcludes,
    MisdemeanorCasesIncludesExcludes,
    MixedCaseloadNumeratorIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.person import (
    FemaleBiologicalSexIncludesExcludes,
    MaleBiologicalSexIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.prosecution import (
    ProsecutionAdministrativeStaffIncludesExcludes,
    ProsecutionAdvocateStaffIncludesExcludes,
    ProsecutionCaseloadDenominatorIncludesExcludes,
    ProsecutionCasesDeclinedIncludesExcludes,
    ProsecutionCasesDisposedIncludesExcludes,
    ProsecutionCasesDivertedOrDeferredIncludesExcludes,
    ProsecutionCasesProsecutedIncludesExcludes,
    ProsecutionCasesReferredIncludesExcludes,
    ProsecutionExpensesPurposeIncludesExcludes,
    ProsecutionExpensesTimeframeIncludesExcludes,
    ProsecutionFelonyCaseloadDenominatorIncludesExcludes,
    ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes,
    ProsecutionFundingGrantsIncludesExcludes,
    ProsecutionFundingPurposeIncludesExcludes,
    ProsecutionFundingStateAppropriationsIncludesExcludes,
    ProsecutionFundingTimeframeIncludesExcludes,
    ProsecutionInvestigativeStaffIncludesExcludes,
    ProsecutionLegalStaffIncludesExcludes,
    ProsecutionMisdemeanorCaseloadDenominatorIncludesExcludes,
    ProsecutionMixedCaseloadDenominatorIncludesExcludes,
    ProsecutionPersonnelExpensesIncludesExcludes,
    ProsecutionStaffIncludesExcludes,
    ProsecutionTrainingExpensesIncludesExcludes,
    ProsecutionVacantStaffIncludesExcludes,
    ProsecutionViolationsIncludesExcludes,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    IncludesExcludesSet,
    MetricCategory,
    MetricDefinition,
)
from recidiviz.justice_counts.utils.constants import MetricUnit
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    ReportingFrequency,
    System,
)

funding = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of the prosecution office to process criminal cases.",
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionFundingTimeframeIncludesExcludes,
            description="Funding timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=ProsecutionFundingPurposeIncludesExcludes,
            description="Funding purpose",
            excluded_set={
                ProsecutionFundingPurposeIncludesExcludes.NON_CRIMINAL_CASE_PROCESSING,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=False,
            dimension_to_description={
                FundingType.STATE_APPROPRIATIONS: "The amount of funding appropriated by the state for the operation and maintenance of the prosecutor’s office to process criminal cases.",
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATIONS: "The amount of funding appropriated by counties or municipalities for the operation and maintenance of the prosecutor’s office to process criminal cases.",
                FundingType.GRANTS: "The amount of funding derived by the office through grants and awards to be used for the operation and maintenance of the prosecutor’s office to process criminal cases.",
                FundingType.OTHER: "The amount of funding to be used for the operation and maintenance of the prosecutor’s office to process criminal cases that is not appropriations from the state, appropriations from counties or cities, or funding from grants.",
                FundingType.UNKNOWN: "The amount of funding for the operation and maintenance of the prosecutor’s office to process criminal cases for which the source is not known.",
            },
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATIONS: [
                    IncludesExcludesSet(
                        members=ProsecutionFundingStateAppropriationsIncludesExcludes,
                        excluded_set={
                            ProsecutionFundingStateAppropriationsIncludesExcludes.PROPOSED,
                            ProsecutionFundingStateAppropriationsIncludesExcludes.PRELIMINARY,
                            ProsecutionFundingStateAppropriationsIncludesExcludes.GRANTS,
                        },
                    ),
                ],
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATIONS: [
                    IncludesExcludesSet(
                        members=ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes,
                        excluded_set={
                            ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes.PROPOSED,
                            ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes.PRELIMINARY,
                        },
                    ),
                ],
                FundingType.GRANTS: [
                    IncludesExcludesSet(
                        members=ProsecutionFundingGrantsIncludesExcludes,
                    ),
                ],
            },
        ),
    ],
)


expenses = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.EXPENSES,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Expenses",
    description="The amount spent by the office for the operation and maintenance of the prosecutor’s office to process criminal cases.",
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionExpensesTimeframeIncludesExcludes,
            description="Expenses timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=ProsecutionExpensesPurposeIncludesExcludes,
            description="Expense type",
            excluded_set={ProsecutionExpensesPurposeIncludesExcludes.NON_CRIMINAL},
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            required=False,
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount spent to employ personnel involved in the operation and maintenance of the prosecutor’s office to process criminal cases.",
                ExpenseType.TRAINING: "The amount spent by the office on the training of personnel involved in the operation and maintenance of the prosecutor’s office to process criminal cases, including any associated expenses, such as registration fees and travel costs.",
                ExpenseType.FACILITIES: "The amount spent by the office for the purchase and use of the physical plant and property owned and operated by the office to process criminal cases.",
                ExpenseType.OTHER: "The amount spent by the office to process criminal cases on other costs relating to the operation and maintenance of the prosecutor’s office that are not personnel, training, or facilities and equipment expenses.",
                ExpenseType.UNKNOWN: "The amount spent by the office to process criminal cases on costs relating to the operation and maintenance of the prosecutor’s office for a purpose that is not known.",
            },
            dimension_to_includes_excludes={
                ExpenseType.PERSONNEL: [
                    IncludesExcludesSet(
                        members=ProsecutionPersonnelExpensesIncludesExcludes,
                        excluded_set={
                            ProsecutionPersonnelExpensesIncludesExcludes.DEFENSE
                        },
                    ),
                ],
                ExpenseType.TRAINING: [
                    IncludesExcludesSet(
                        members=ProsecutionTrainingExpensesIncludesExcludes,
                        excluded_set={
                            ProsecutionTrainingExpensesIncludesExcludes.FREE_PROGRAMS
                        },
                    ),
                ],
                ExpenseType.FACILITIES: [
                    IncludesExcludesSet(
                        members=FacilitiesAndEquipmentExpensesIncludesExcludes
                    ),
                ],
            },
        )
    ],
)


staff = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the office to process criminal cases.",
    unit=MetricUnit.FULL_TIME,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            ProsecutionStaffIncludesExcludes,
            excluded_set={
                ProsecutionStaffIncludesExcludes.VOLUNTEER,
                ProsecutionStaffIncludesExcludes.INTERN,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_description={
                StaffType.LEGAL_STAFF: "The number of full-time equivalent positions that are responsible for their own criminal caseload or for performing tasks that have a legal function in support of that caseload.",
                StaffType.ADVOCATE_STAFF: "The number of full-time equivalent positions that advise, counsel, or assist victims or witnesses of crime.",
                StaffType.ADMINISTRATIVE: "The number of full-time equivalent positions that support legal and clerical policies and logistics to process criminal cases.",
                StaffType.INVESTIGATIVE_STAFF: "The number of full-time equivalent positions that are responsible for gathering evidence to support criminal prosecutorial cases, inquiries into the details of a criminal case, and gathering evidence.",
                StaffType.OTHER: "The number of full-time equivalent positions to process criminal cases that are not legal staff, victim-witness advocate staff, administrative staff, investigative staff, or staff with unknown position types but are another type of staff position.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions to process criminal cases that are of an unknown type.",
                StaffType.VACANT_POSITIONS: "The number of full-time equivalent positions to process criminal cases of any type that are budgeted but not currently filled.",
            },
            dimension_to_includes_excludes={
                StaffType.LEGAL_STAFF: [
                    IncludesExcludesSet(members=ProsecutionLegalStaffIncludesExcludes),
                ],
                StaffType.ADVOCATE_STAFF: [
                    IncludesExcludesSet(
                        members=ProsecutionAdvocateStaffIncludesExcludes
                    ),
                ],
                StaffType.ADMINISTRATIVE: [
                    IncludesExcludesSet(
                        members=ProsecutionAdministrativeStaffIncludesExcludes,
                        excluded_set={
                            ProsecutionAdministrativeStaffIncludesExcludes.INVESTIGATIVE_STAFF
                        },
                    ),
                ],
                StaffType.INVESTIGATIVE_STAFF: [
                    IncludesExcludesSet(
                        members=ProsecutionInvestigativeStaffIncludesExcludes,
                    ),
                ],
                StaffType.VACANT_POSITIONS: [
                    IncludesExcludesSet(
                        members=ProsecutionVacantStaffIncludesExcludes,
                        excluded_set={ProsecutionVacantStaffIncludesExcludes.FILLED},
                    ),
                ],
            },
        )
    ],
)

cases_declined = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DECLINED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Declined",
    description="The number of criminal cases referred to the office for review and declined for prosecution.",
    additional_description="If the same person is listed as the defendant in multiple cases, these cases should be counted separately if they were referred and reviewed on different dates. If multiple charges were referred against one person on the same date, with the expectation that they would be reviewed and filed together, these charges should be combined to count as one case. If a single case includes multiple defendants, it should be counted as one case.",
    unit=MetricUnit.CASES,
    measurement_type=MeasurementType.DELTA,
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionCasesDeclinedIncludesExcludes,
            excluded_set={ProsecutionCasesDeclinedIncludesExcludes.INTERNAL_TRANSFER},
        ),
    ],
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            # TODO(#18071)
            dimension=CaseDeclinedSeverityType,
            required=False,
            dimension_to_description={
                CaseDeclinedSeverityType.FELONY: "The number of cases referred and declined in which the leading charge was for a felony offense, as defined by the state statute.",
                CaseDeclinedSeverityType.MISDEMEANOR: "The number of cases referred and declined in which the leading charge was for a misdemeanor offense, as defined by the state statute.",
                CaseDeclinedSeverityType.OTHER: "The number of criminal cases referred and declined in which the leading charge was for another offense that was not a felony or misdemeanor.",
                CaseDeclinedSeverityType.UNKNOWN: "The number of criminal cases referred and declined in which the leading charge was for an offense of unknown severity.",
            },
            dimension_to_includes_excludes={
                CaseDeclinedSeverityType.FELONY: [
                    IncludesExcludesSet(
                        members=FelonyCasesIncludesExcludes,
                        excluded_set={FelonyCasesIncludesExcludes.MISDEMEANOR},
                    ),
                ],
                CaseDeclinedSeverityType.MISDEMEANOR: [
                    IncludesExcludesSet(
                        members=MisdemeanorCasesIncludesExcludes,
                        excluded_set={
                            MisdemeanorCasesIncludesExcludes.INFRACTION,
                            MisdemeanorCasesIncludesExcludes.FELONY,
                        },
                    ),
                ],
            },
        ),
        AggregatedDimension(
            # TODO(#18071)
            dimension=BiologicalSex,
            required=False,
            dimension_to_description={
                BiologicalSex.MALE: "The number of criminal cases referred to the office for review and declined for prosecution with a defendant whose biological sex is male.",
                BiologicalSex.FEMALE: "The number of criminal cases referred to the office for review and declined for prosecution with a defendant whose biological sex is female.",
                BiologicalSex.UNKNOWN: "The number of criminal cases referred to the office for review and declined for prosecution with a defendant whose biological sex is not known.",
            },
            dimension_to_includes_excludes={
                BiologicalSex.MALE: [
                    IncludesExcludesSet(
                        members=MaleBiologicalSexIncludesExcludes,
                        excluded_set={MaleBiologicalSexIncludesExcludes.UNKNOWN},
                    ),
                ],
                BiologicalSex.FEMALE: [
                    IncludesExcludesSet(
                        members=FemaleBiologicalSexIncludesExcludes,
                        excluded_set={FemaleBiologicalSexIncludesExcludes.UNKNOWN},
                    ),
                ],
            },
        ),
        # TODO(#18071)
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=False,
            contexts=[Context(key=ContextKey.OTHER_RACE_DESCRIPTION, label="")],
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "The number of criminal cases referred to the office of people whose race is listed as Native American, American Indian, Native Alaskan, or similar that were reviewed and declined for prosecution. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "The number of criminal cases referred to the office of people whose race is listed as Asian that were reviewed and declined for prosecution. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "The number of criminal cases referred to the office of people whose race is listed as Black or African-American that were reviewed and declined for prosecution. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.MORE_THAN_ONE_RACE: "The number of criminal cases referred to the office of people whose race and ethnicity are listed as Hispanic or Latino that were reviewed and declined for prosecution. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "The number of criminal cases referred to the office of people whose race is listed as Native Hawaiian, Pacific Islander, or similar that were reviewed and declined for prosecution. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "The number of criminal cases referred to the office of people whose race is listed as some other race, not included above that were reviewed and declined for prosecution.",
                CensusRace.WHITE: "The number of criminal cases referred to the office of people whose race is listed as White, Caucasian, or Anglo that were reviewed and declined for prosecution. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
                CensusRace.UNKNOWN: "The number of criminal cases referred to the office of people whose race is not known that were reviewed and declined for prosecution.",
                CensusRace.HISPANIC_OR_LATINO: "The number of criminal cases referred to the office of people whose race and ethnicity are listed as Hispanic or Latino that were reviewed and declined for prosecution. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
            },
        ),
    ],
)

cases_referred = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_REFERRED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Referred",
    description="The number of criminal cases referred to the office.",
    additional_description="If the same person is listed as the defendant in multiple cases, these cases should be counted separately if they were referred and reviewed on different dates. If multiple charges were referred against one person on the same date, with the expectation that they would be reviewed and filed together, these charges should be combined to count as one case. If a single case includes multiple defendants, it should be counted as one case.",
    unit=MetricUnit.CASES_PER_ATTORNEY,
    measurement_type=MeasurementType.DELTA,
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionCasesReferredIncludesExcludes,
            excluded_set={
                ProsecutionCasesReferredIncludesExcludes.INTERNAL_TRANSFER,
                ProsecutionCasesReferredIncludesExcludes.REOPENED,
            },
        ),
    ],
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            # TODO(#18071)
            dimension=ReferredCaseSeverityType,
            required=False,
            dimension_to_description={
                ReferredCaseSeverityType.FELONY: "The number of criminal cases referred to the office in which the leading charge was for a felony offense.",
                ReferredCaseSeverityType.MISDEMEANOR: "The number of criminal cases referred to the office in which the leading charge was for a felony offense.",
                ReferredCaseSeverityType.OTHER: "The number of criminal cases referred to the office in which the leading charge was not for a felony or misdemeanor offense.",
                ReferredCaseSeverityType.UNKNOWN: "The number of criminal cases referred to the office in which the leading charge was for an offense of unknown severity.",
            },
            dimension_to_includes_excludes={
                ReferredCaseSeverityType.FELONY: [
                    IncludesExcludesSet(
                        members=FelonyCasesIncludesExcludes,
                        excluded_set={FelonyCasesIncludesExcludes.MISDEMEANOR},
                    ),
                ],
                ReferredCaseSeverityType.MISDEMEANOR: [
                    IncludesExcludesSet(
                        members=MisdemeanorCasesIncludesExcludes,
                        excluded_set={
                            MisdemeanorCasesIncludesExcludes.FELONY,
                            MisdemeanorCasesIncludesExcludes.INFRACTION,
                        },
                    ),
                ],
            },
        )
    ],
)

cases_prosecuted = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_PROSECUTED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Prosecuted",
    description="The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office.",
    unit=MetricUnit.CASES,
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionCasesProsecutedIncludesExcludes,
            excluded_set={ProsecutionCasesProsecutedIncludesExcludes.UNASSIGNED},
        ),
    ],
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    measurement_type=MeasurementType.DELTA,
    aggregated_dimensions=[
        AggregatedDimension(
            # TODO(#18071)
            dimension=ProsecutedCaseSeverityType,
            required=False,
            dimension_to_description={
                ProsecutedCaseSeverityType.FELONY: "The number of cases prosecuted in which the leading charge was for a felony offense.",
                ProsecutedCaseSeverityType.MISDEMEANOR: "The number of cases prosecuted in which the leading charge was for a misdemeanor offense.",
                ProsecutedCaseSeverityType.OTHER: "The number of cases prosecuted in which the leading charge was for another offense that was not a felony or misdemeanor.",
                ProsecutedCaseSeverityType.UNKNOWN: "The number of cases prosecuted in which the leading charge was for an offense of unknown severity.",
            },
            dimension_to_includes_excludes={
                ProsecutedCaseSeverityType.FELONY: [
                    IncludesExcludesSet(
                        members=FelonyCasesIncludesExcludes,
                        excluded_set={FelonyCasesIncludesExcludes.MISDEMEANOR},
                    ),
                ],
                ProsecutedCaseSeverityType.MISDEMEANOR: [
                    IncludesExcludesSet(
                        members=MisdemeanorCasesIncludesExcludes,
                        excluded_set={
                            MisdemeanorCasesIncludesExcludes.INFRACTION,
                            MisdemeanorCasesIncludesExcludes.FELONY,
                        },
                    ),
                ],
            },
        ),
        AggregatedDimension(
            # TODO(#18071)
            dimension=BiologicalSex,
            required=False,
            dimension_to_description={
                BiologicalSex.MALE: "The number of cases assigned for prosecution to an attorney and prosecuted by the office with a defendant whose biological sex is male.",
                BiologicalSex.FEMALE: "The number of cases assigned for prosecution to an attorney and prosecuted by the office with a defendant whose biological sex is female.",
                BiologicalSex.UNKNOWN: "The number of cases assigned for prosecution to an attorney and prosecuted by the office with a defendant whose biological sex is not known.",
            },
            dimension_to_includes_excludes={
                BiologicalSex.MALE: [
                    IncludesExcludesSet(
                        members=MaleBiologicalSexIncludesExcludes,
                        excluded_set={MaleBiologicalSexIncludesExcludes.UNKNOWN},
                    ),
                ],
                BiologicalSex.FEMALE: [
                    IncludesExcludesSet(
                        members=FemaleBiologicalSexIncludesExcludes,
                        excluded_set={FemaleBiologicalSexIncludesExcludes.UNKNOWN},
                    ),
                ],
            },
        ),
        # TODO(#18071)
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=False,
            contexts=[Context(key=ContextKey.OTHER_RACE_DESCRIPTION, label="")],
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.HISPANIC_OR_LATINO: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                CensusRace.MORE_THAN_ONE_RACE: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race is listed as more than one race, such as White and Black.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race is listed as some other race, not included above.",
                CensusRace.UNKNOWN: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race is not known.",
                CensusRace.WHITE: "The number of criminal cases assigned for prosecution to an attorney and prosecuted by the office of people whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa. ",
            },
        ),
    ],
)

caseload_numerator = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASELOADS_PEOPLE,
    category=MetricCategory.POPULATIONS,
    display_name="Open Cases",
    description="The number of people with open criminal cases carried by the office (used as the numerator in the calculation of the caseload metric).",
    unit=MetricUnit.CASELOAD,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=CaseloadNumeratorIncludesExcludes,
            excluded_set={CaseloadNumeratorIncludesExcludes.NOT_ASSIGNED},
        ),
    ],
    aggregated_dimensions=[
        # TODO(#18071)
        AggregatedDimension(
            dimension=CaseSeverityType,
            required=False,
            dimension_to_description={
                CaseSeverityType.FELONY: "The number of people with open felony cases.",
                CaseSeverityType.MISDEMEANOR: "The number of people with open misdemeanor cases.",
                CaseSeverityType.MIXED: "The number of people with open felony and misdemeanor cases.",
                CaseSeverityType.OTHER: "The number of people with open criminal cases.",
                CaseSeverityType.UNKNOWN: "The number of people with open criminal cases of unknown severity.",
            },
            dimension_to_includes_excludes={
                CaseSeverityType.FELONY: [
                    IncludesExcludesSet(
                        members=FelonyCaseloadNumeratorIncludesExcludes,
                        excluded_set={
                            FelonyCaseloadNumeratorIncludesExcludes.UNASSIGNED_CASES,
                        },
                    ),
                ],
                CaseSeverityType.MISDEMEANOR: [
                    IncludesExcludesSet(
                        members=MisdemeanorCaseloadNumeratorIncludesExcludes,
                        excluded_set={
                            MisdemeanorCaseloadNumeratorIncludesExcludes.UNASSIGNED_CASES,
                        },
                    ),
                ],
                CaseSeverityType.MIXED: [
                    IncludesExcludesSet(
                        members=MixedCaseloadNumeratorIncludesExcludes,
                        excluded_set={
                            MixedCaseloadNumeratorIncludesExcludes.UNASSIGNED_FELONY_CASES,
                            MixedCaseloadNumeratorIncludesExcludes.UNASSIGNED_MISDEMEANOR_CASES,
                        },
                    ),
                ],
            },
        )
    ],
)


caseload_denominator = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASELOADS_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff with Caseload",
    description="The number of legal staff carrying a criminal caseload (used as the denominator in the calculation of the caseload metric).",
    unit=MetricUnit.CASELOAD,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionCaseloadDenominatorIncludesExcludes,
        ),
    ],
    aggregated_dimensions=[
        # TODO(#18071)
        AggregatedDimension(
            dimension=CaseSeverityType,
            required=False,
            dimension_to_description={
                CaseSeverityType.FELONY: "The number of staff carrying a felony caseload.",
                CaseSeverityType.MISDEMEANOR: "The number of staff carrying a misdemeanor caseload.",
                CaseSeverityType.MIXED: "The number of staff carrying a mixed (felony and misdemeanor) caseload.",
                CaseSeverityType.OTHER: "The number of staff carrying a criminal caseload that does not comprise felony or misdemeanor cases.",
                CaseSeverityType.UNKNOWN: "The number of staff carrying a criminal caseload of unknown severity.",
            },
            dimension_to_includes_excludes={
                CaseSeverityType.FELONY: [
                    IncludesExcludesSet(
                        members=ProsecutionFelonyCaseloadDenominatorIncludesExcludes,
                    ),
                ],
                CaseSeverityType.MISDEMEANOR: [
                    IncludesExcludesSet(
                        members=ProsecutionMisdemeanorCaseloadDenominatorIncludesExcludes,
                    ),
                ],
                CaseSeverityType.MIXED: [
                    IncludesExcludesSet(
                        members=ProsecutionMixedCaseloadDenominatorIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)

cases_diverted_or_deferred = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DIVERTED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Diverted/Deferred",
    description="The number of criminal cases diverted from traditional case processing.",
    additional_description="This may include cases diverted before or after filing, cases reopened and diverted, or cases deferred in lieu of probation conditions. Diversion programs will vary by jurisdiction and may include diversion to specialty court dockets.",
    unit=MetricUnit.CASES,
    measurement_type=MeasurementType.DELTA,
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionCasesDivertedOrDeferredIncludesExcludes,
            excluded_set={ProsecutionCasesDivertedOrDeferredIncludesExcludes.RETAINED},
        ),
    ],
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        # TODO(#18071)
        AggregatedDimension(
            dimension=DivertedCaseSeverityType,
            required=False,
            dimension_to_description={
                DivertedCaseSeverityType.FELONY: "The number of criminal cases diverted or deferred in which the leading charge was for a felony offense.",
                DivertedCaseSeverityType.MISDEMEANOR: "The number of criminal cases diverted or deferred in which the leading charge was for a misdemeanor offense.",
                DivertedCaseSeverityType.OTHER: "The number of criminal cases diverted or deferred in which the leading charge was for another offense that was not a felony or misdemeanor.",
                DivertedCaseSeverityType.UNKNOWN: "The number of criminal cases diverted or deferred in which the leading charge was for an offense of unknown severity.",
            },
            dimension_to_includes_excludes={
                DivertedCaseSeverityType.FELONY: [
                    IncludesExcludesSet(
                        members=FelonyCasesIncludesExcludes,
                        excluded_set={FelonyCasesIncludesExcludes.MISDEMEANOR},
                    ),
                ],
                DivertedCaseSeverityType.MISDEMEANOR: [
                    IncludesExcludesSet(
                        members=MisdemeanorCasesIncludesExcludes,
                        excluded_set={
                            MisdemeanorCasesIncludesExcludes.INFRACTION,
                            MisdemeanorCasesIncludesExcludes.FELONY,
                        },
                    ),
                ],
            },
        ),
        AggregatedDimension(
            # TODO(#18071)
            dimension=BiologicalSex,
            required=False,
            dimension_to_description={
                BiologicalSex.MALE: "The number of cases diverted from traditional case processing and closed by the office with a defendant whose biological sex is male.",
                BiologicalSex.FEMALE: "The number of cases diverted from traditional case processing and closed by the office with a defendant whose biological sex is female.",
                BiologicalSex.UNKNOWN: "The number of cases diverted from traditional case processing and closed by the office with a defendant whose biological sex is not known.",
            },
            dimension_to_includes_excludes={
                BiologicalSex.MALE: [
                    IncludesExcludesSet(
                        members=MaleBiologicalSexIncludesExcludes,
                        excluded_set={MaleBiologicalSexIncludesExcludes.UNKNOWN},
                    ),
                ],
                BiologicalSex.FEMALE: [
                    IncludesExcludesSet(
                        members=FemaleBiologicalSexIncludesExcludes,
                        excluded_set={FemaleBiologicalSexIncludesExcludes.UNKNOWN},
                    ),
                ],
            },
        ),
        # TODO(#18071)
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=False,
            contexts=[Context(key=ContextKey.OTHER_RACE_DESCRIPTION, label="")],
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "The number of criminal cases diverted from traditional case processing of people whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "The number of criminal cases diverted from traditional case processing of people whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "The number of criminal cases diverted from traditional case processing of people whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.HISPANIC_OR_LATINO: "The number of criminal cases diverted from traditional case processing of people whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                CensusRace.MORE_THAN_ONE_RACE: "The number of criminal cases diverted from traditional case processing of people whose race is listed as more than one race, such as White and Black.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "The number of criminal cases diverted from traditional case processing of people whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "The number of criminal cases diverted from traditional case processing of people whose race is listed as some other race, not included above.",
                CensusRace.UNKNOWN: "The number of criminal cases diverted from traditional case processing of people whose race is not known.",
                CensusRace.WHITE: "The number of criminal cases diverted from traditional case processing of people whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
            },
        ),
    ],
)
cases_disposed = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DISPOSED,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Cases Disposed",
    description="The number of criminal cases disposed by the office.",
    additional_description="If the same person is listed as the defendant in multiple cases, these cases should be counted separately if they were disposed on different dates.",
    unit=MetricUnit.CASES,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionCasesDisposedIncludesExcludes,
            excluded_set={
                ProsecutionCasesDisposedIncludesExcludes.INACTIVE,
                ProsecutionCasesDisposedIncludesExcludes.PENDING,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=DispositionType,
            required=False,
            dimension_to_description={
                DispositionType.DISMISSAL: "The number of criminal cases dismissed after filing and closed by the office.",
                DispositionType.PLEA: "The number of criminal cases resulting in conviction by guilty plea and closed by the office.",
                DispositionType.TRIAL: "The number of criminal cases resolved at trial and closed by the office.",
                DispositionType.OTHER: "The number of criminal cases disposed by the office that were not dismissed, resolved by plea, or resolved at trial but disposed by another means.",
                DispositionType.UNKNOWN: "The number of criminal cases disposed by the office for which the disposition method is unknown.",
            },
            dimension_to_includes_excludes={
                DispositionType.DISMISSAL: [
                    IncludesExcludesSet(members=CasesDismissedIncludesExcludes),
                ],
                DispositionType.PLEA: [
                    IncludesExcludesSet(members=CasesResolvedByPleaIncludesExcludes),
                ],
                DispositionType.TRIAL: [
                    IncludesExcludesSet(members=CasesResolvedAtTrialIncludesExcludes),
                ],
            },
        )
    ],
)

violations = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.VIOLATIONS_WITH_DISCIPLINARY_ACTION,
    category=MetricCategory.FAIRNESS,
    display_name="Violations Filed Resulting in Discipline",
    description="The number of violations filed against any attorney with a criminal caseload in the office that resulted in a disciplinary action imposed on the attorney by the local or state disciplinary board during the time period.",
    unit=MetricUnit.VIOLATIONS,
    measurement_type=MeasurementType.DELTA,
    includes_excludes=[
        IncludesExcludesSet(
            members=ProsecutionViolationsIncludesExcludes,
            excluded_set={
                ProsecutionViolationsIncludesExcludes.NO_DISCIPLINARY_ACTION,
                ProsecutionViolationsIncludesExcludes.INFORMAL,
                ProsecutionViolationsIncludesExcludes.PENDING,
                ProsecutionViolationsIncludesExcludes.DUPLICATE,
            },
        ),
    ],
    reporting_frequencies=[ReportingFrequency.ANNUAL],
)
