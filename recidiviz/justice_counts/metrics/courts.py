# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Defines all Justice Counts metrics for Courts and Pretrial."""

from recidiviz.justice_counts.dimensions.common import DispositionType, ExpenseType
from recidiviz.justice_counts.dimensions.courts import (
    CaseSeverityType,
    FundingType,
    ReleaseType,
    SentenceType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    CensusRace,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.common import (
    CasesDismissedIncludesExcludes,
    CasesResolvedAtTrialIncludesExcludes,
    CasesResolvedByPleaIncludesExcludes,
    CountyOrMunicipalAppropriationIncludesExcludes,
    GrantsIncludesExcludes,
    StaffIncludesExcludes,
    StateAppropriationIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.courts import (
    CasesDisposedIncludesExcludes,
    CommunitySupervisionOnlySentencesIncludesExcludes,
    CriminalCaseFilingsIncludesExcludes,
    ExpensesFacilitiesAndEquipmentIncludesExcludes,
    ExpensesPersonnelIncludesExcludes,
    ExpensesPurposeIncludesExcludes,
    ExpensesTimeframeIncludesExcludes,
    ExpensesTrainingIncludesExcludes,
    FelonyCriminalCaseFilingsIncludesExcludes,
    FinesOrFeesOnlySentencesIncludesExcludes,
    FundingPurposeIncludesExcludes,
    FundingTimeframeIncludesExcludes,
    JailSentencesIncludesExcludes,
    JudgesIncludesExcludes,
    LegalStaffIncludesExcludes,
    MisdemeanorOrInfractionCriminalCaseFilingsIncludesExcludes,
    NewOffensesWhileOnPretrialReleaseIncludesExcludes,
    PretrialReleasesIncludesExcludes,
    PretrialReleasesMonetaryBailIncludesExcludes,
    PretrialReleasesNonMonetaryBailIncludesExcludes,
    PretrialReleasesOnOwnRecognizanceIncludesExcludes,
    PrisonSentencesIncludesExcludes,
    SecurityStaffIncludesExcludes,
    SentencesImposedIncludesExcludes,
    SplitSentencesIncludesExcludes,
    SupportOrAdministrativeStaffIncludesExcludes,
    SuspendedSentencesIncludesExcludes,
    VacantPositionsIncludesExcludes,
    VictimAdvocateStaffIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.person import (
    FemaleBiologicalSexIncludesExcludes,
    MaleBiologicalSexIncludesExcludes,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    IncludesExcludesSet,
    MetricCategory,
    MetricDefinition,
    ReportingFrequency,
)
from recidiviz.justice_counts.utils.constants import MetricUnit
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    System,
)

funding = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of the court system to process criminal cases.",
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=FundingTimeframeIncludesExcludes,
            description="Funding timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=FundingPurposeIncludesExcludes,
            description="Funding purpose",
            excluded_set={
                FundingPurposeIncludesExcludes.SUPERVISION_OPERATIONS,
                FundingPurposeIncludesExcludes.JUVENILE,
                FundingPurposeIncludesExcludes.NON_COURT_FUNCTIONS,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=True,
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATION: [
                    IncludesExcludesSet(
                        members=StateAppropriationIncludesExcludes,
                        excluded_set={
                            StateAppropriationIncludesExcludes.PROPOSED,
                            StateAppropriationIncludesExcludes.PRELIMINARY,
                            StateAppropriationIncludesExcludes.GRANTS,
                        },
                    ),
                ],
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATION: [
                    IncludesExcludesSet(
                        members=CountyOrMunicipalAppropriationIncludesExcludes,
                        excluded_set={
                            CountyOrMunicipalAppropriationIncludesExcludes.PROPOSED,
                            CountyOrMunicipalAppropriationIncludesExcludes.PRELIMINARY,
                        },
                    ),
                ],
                FundingType.GRANTS: [
                    IncludesExcludesSet(
                        members=GrantsIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                FundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for the operation and maintenance of the court system’s criminal case processing.",
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATION: "The amount of funding counties or municipalities appropriated for the operation and maintenance of the court system’s criminal case processing.",
                FundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for the operation and maintenance of the court system’s criminal case processing.",
                FundingType.OTHER: "The amount of funding for the operation and maintenance of the court that is not appropriations from the state, appropriations from counties or cities, or funding from grants.",
                FundingType.UNKNOWN: "The amount of funding to be used for the operation and maintenance of the court for which the source is not known.",
            },
        )
    ],
)

expenses = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.EXPENSES,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Expenses",
    description="The amount spent by the court system for the operation and maintenance of the court system to process criminal cases.",
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=ExpensesTimeframeIncludesExcludes,
            description="Expenses timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=ExpensesPurposeIncludesExcludes,
            description="Expense purpose",
            excluded_set={
                ExpensesPurposeIncludesExcludes.COMMUNITY_SUPERVISION_OPERATIONS,
                ExpensesPurposeIncludesExcludes.JUVENILE,
                ExpensesPurposeIncludesExcludes.NON_COURT_FUNCTIONS,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            required=True,
            dimension_to_includes_excludes={
                ExpenseType.PERSONNEL: [
                    IncludesExcludesSet(
                        members=ExpensesPersonnelIncludesExcludes,
                        excluded_set={
                            ExpensesPersonnelIncludesExcludes.COMPANIES_CONTRACTED,
                        },
                    ),
                ],
                ExpenseType.TRAINING: [
                    IncludesExcludesSet(
                        members=ExpensesTrainingIncludesExcludes,
                        excluded_set={
                            ExpensesTrainingIncludesExcludes.NO_COST_COURSES,
                        },
                    ),
                ],
                ExpenseType.FACILITIES: [
                    IncludesExcludesSet(
                        members=ExpensesFacilitiesAndEquipmentIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount spent by the court to employ personnel involved in the operation and maintenance of the court for the processing of criminal cases.",
                ExpenseType.TRAINING: "The amount spent by the court on the training of personnel and staff involved in the operation and maintenance of the court for the processing of criminal cases, including any associated expenses, such as registration fees and travel costs.",
                ExpenseType.FACILITIES: "The amount the court spent for the purchase and use of the physical plant and property owned and operated by the court for the processing of criminal cases.",
                ExpenseType.OTHER: "The amount spent by the court on other costs relating to the operation and maintenance of the court for the processing of criminal cases that are not personnel, training, or facilities and equipment expenses.",
                ExpenseType.UNKNOWN: "The amount spent by the court on other costs relating to the operation and maintenance of the court for the processing of criminal cases for a purpose that is not known.",
            },
        )
    ],
)

judges_and_staff = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Judges and Staff",
    description="The number of full-time equivalent positions budgeted and paid for by the court system for criminal case processing.",
    unit=MetricUnit.FULL_TIME,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=StaffIncludesExcludes,
            excluded_set={
                StaffIncludesExcludes.VOLUNTEER,
                StaffIncludesExcludes.INTERN,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_includes_excludes={
                StaffType.JUDGES: [
                    IncludesExcludesSet(
                        members=JudgesIncludesExcludes,
                        excluded_set={
                            JudgesIncludesExcludes.WITHOUT_CRIMINAL_CASE,
                        },
                    ),
                ],
                StaffType.LEGAL: [
                    IncludesExcludesSet(
                        members=LegalStaffIncludesExcludes,
                        excluded_set={
                            LegalStaffIncludesExcludes.JUDGES,
                        },
                    ),
                ],
                StaffType.SECURITY: [
                    IncludesExcludesSet(
                        members=SecurityStaffIncludesExcludes,
                    ),
                ],
                StaffType.ADMINISTRATIVE: [
                    IncludesExcludesSet(
                        members=SupportOrAdministrativeStaffIncludesExcludes,
                    ),
                ],
                StaffType.ADVOCATE: [
                    IncludesExcludesSet(
                        members=VictimAdvocateStaffIncludesExcludes,
                        excluded_set={
                            VictimAdvocateStaffIncludesExcludes.NOT_FULL_TIME,
                        },
                    ),
                ],
                StaffType.VACANT: [
                    IncludesExcludesSet(
                        members=VacantPositionsIncludesExcludes,
                        excluded_set={
                            VacantPositionsIncludesExcludes.FILLED,
                        },
                    ),
                ],
            },
            dimension_to_description={
                StaffType.JUDGES: "The number of full-time equivalent positions for judges for criminal case processing.",
                StaffType.LEGAL: "The number of full-time equivalent positions for criminal case processing that are not judges and are responsible for legal work.",
                StaffType.SECURITY: "The number of full-time equivalent positions for criminal case processing that are responsible for the safety of the court and people within the court system’s facilities.",
                StaffType.ADMINISTRATIVE: "The number of full-time equivalent positions for criminal case processing that assist in the organization, logistics, and management of the court system.",
                StaffType.ADVOCATE: "The number of full-time equivalent positions for criminal case processing that provide victim support services.",
                StaffType.OTHER: "The number of full-time equivalent positions for criminal case filings that are not judges, legal staff, security staff, support or administrative staff, or victim advocate staff.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions for criminal case processing that are of an unknown type.",
                StaffType.VACANT: "The number of full-time equivalent positions for criminal case processing of any type that are budgeted but not currently filled.",
            },
        )
    ],
)

pretrial_releases = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.PRETRIAL_RELEASES,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Pretrial Releases",
    description="The number of people released while awaiting disposition in a criminal case.",
    additional_description="If the same person is listed as the defendant in multiple cases, these cases should be counted separately if they were referred and reviewed on different dates. If multiple charges were referred against one person on the same date, with the expectation that they would be reviewed and filed together, these charges should be combined to count as one case. If a single case includes multiple defendants, it should be counted as one case.",
    unit=MetricUnit.PRETRIAL_RELEASES,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=PretrialReleasesIncludesExcludes,
            excluded_set={
                PretrialReleasesIncludesExcludes.AWAITING_DISPOSITION,
                PretrialReleasesIncludesExcludes.TRANSFERRED,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ReleaseType,
            required=False,
            dimension_to_includes_excludes={
                ReleaseType.ON_OWN: [
                    IncludesExcludesSet(
                        members=PretrialReleasesOnOwnRecognizanceIncludesExcludes,
                        excluded_set={
                            PretrialReleasesOnOwnRecognizanceIncludesExcludes.BEFORE_BAIL_HEARING,
                            PretrialReleasesOnOwnRecognizanceIncludesExcludes.AWAITING_DISPOSITION,
                            PretrialReleasesOnOwnRecognizanceIncludesExcludes.TRANSFERRED,
                        },
                    ),
                ],
                ReleaseType.MONETARY_BAIL: [
                    IncludesExcludesSet(
                        members=PretrialReleasesMonetaryBailIncludesExcludes,
                        excluded_set={
                            PretrialReleasesMonetaryBailIncludesExcludes.BEFORE_BAIL_HEARING,
                        },
                    ),
                ],
                ReleaseType.NON_MONETARY_BAIL: [
                    IncludesExcludesSet(
                        members=PretrialReleasesNonMonetaryBailIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                ReleaseType.ON_OWN: "The number of people released without conditions awaiting disposition in a criminal case.",
                ReleaseType.MONETARY_BAIL: "The number of people released on monetary bail while awaiting disposition in a criminal case.",
                ReleaseType.NON_MONETARY_BAIL: "The number of people released on non-monetary bail while awaiting disposition in a criminal case.",
                ReleaseType.OTHER: "The number of people released while awaiting disposition in a criminal case by a means other than on their own recognizance, on monetary bail, or on non-monetary bail.",
                ReleaseType.UNKNOWN: "The number of people released while awaiting disposition in a criminal case by unknown means.",
            },
        )
    ],
)


sentences_imposed = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.SENTENCES,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Sentences Imposed",
    description="The number of cases in which the court imposed a sentence as a result of a criminal conviction.",
    additional_description="Sentences imposed are counted by the number of cases disposed, not the number of individual convictions or sanctions attached. The case should be categorized based on the most serious sentence imposed in the case. If a person has multiple charges under the same case, it should be counted as one sentence imposed according to the most serious sentence. If a person has multiple cases disposed, each separate case should be counted in this metric.",
    unit=MetricUnit.SENTENCES,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=SentencesImposedIncludesExcludes,
            excluded_set={
                SentencesImposedIncludesExcludes.TRANSFERRED,
                SentencesImposedIncludesExcludes.REINSTATED,
                SentencesImposedIncludesExcludes.CHANGING_PAROLE_STATUS,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=SentenceType,
            required=False,
            dimension_to_includes_excludes={
                SentenceType.PRISON: [
                    IncludesExcludesSet(
                        members=PrisonSentencesIncludesExcludes,
                        excluded_set={
                            PrisonSentencesIncludesExcludes.RETURNS_TO_PRISON,
                            PrisonSentencesIncludesExcludes.SPLIT_SENTENCE,
                        },
                    ),
                ],
                SentenceType.JAIL: [
                    IncludesExcludesSet(
                        members=JailSentencesIncludesExcludes,
                        excluded_set={
                            JailSentencesIncludesExcludes.RETURNS_TO_JAIL,
                            JailSentencesIncludesExcludes.SPLIT_SENTENCE,
                        },
                    ),
                ],
                SentenceType.SPLIT: [
                    IncludesExcludesSet(
                        members=SplitSentencesIncludesExcludes,
                    ),
                ],
                SentenceType.SUSPENDED: [
                    IncludesExcludesSet(
                        members=SuspendedSentencesIncludesExcludes,
                    ),
                ],
                SentenceType.COMMUNITY_SUPERVISION: [
                    IncludesExcludesSet(
                        members=CommunitySupervisionOnlySentencesIncludesExcludes,
                        excluded_set={
                            CommunitySupervisionOnlySentencesIncludesExcludes.SPLIT_SENTENCE,
                            CommunitySupervisionOnlySentencesIncludesExcludes.SUSPENDED_SENTENCE,
                        },
                    ),
                ],
                SentenceType.FINES_FEES: [
                    IncludesExcludesSet(
                        members=FinesOrFeesOnlySentencesIncludesExcludes,
                        excluded_set={
                            FinesOrFeesOnlySentencesIncludesExcludes.CASE_FEES,
                            FinesOrFeesOnlySentencesIncludesExcludes.MONETARY_SANCTIONS,
                            FinesOrFeesOnlySentencesIncludesExcludes.OTHER_FINANCIAL_OBLIGATIONS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                SentenceType.PRISON: "The number of cases disposed with a criminal conviction for which the most serious sentence imposed was incarceration in state prison.",
                SentenceType.JAIL: "The number of cases resolved with a criminal conviction for which the most serious sentence imposed was incarceration in a county jail.",
                SentenceType.SPLIT: "The number of cases resolved with a criminal conviction for which the most serious sentence explicitly imposed was a sentence to incarceration followed by community supervision.",
                SentenceType.SUSPENDED: "The number of cases resolved with a criminal conviction for which the most serious sentence imposed was a term of incarceration, but that term of incarceration is suspended, and the person begins a term of community supervision.",
                SentenceType.COMMUNITY_SUPERVISION: "The number of cases resolved with a criminal conviction for which the most serious sentence imposed was community supervision.",
                SentenceType.FINES_FEES: "The number of cases resolved with a criminal conviction for which the most serious sentence imposed is solely financial obligations to the court.",
                SentenceType.OTHER: "The number of cases resolved by criminal conviction for which the most serious sentence imposed is not prison, jail, split, suspended, community supervision, or solely fines/fees.",
                SentenceType.UNKNOWN: "The number of cases resolved by criminal conviction for which the most serious sentence imposed is unknown.",
            },
        ),
        # TODO(#18071) reuse global includes/excludes
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=False,
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.HISPANIC_OR_LATINO: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                CensusRace.MORE_THAN_ONE_RACE: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race is listed as more than one race, such as White and Black.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race is listed as some other race, not included above.",
                CensusRace.UNKNOWN: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race is not known.",
                CensusRace.WHITE: "The number of cases in which the court imposed a sentence as a result of a criminal conviction of people whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
            },
        ),
        # TODO(#18071) reuse global includes/excludes
        AggregatedDimension(
            dimension=BiologicalSex,
            required=False,
            dimension_to_includes_excludes={
                BiologicalSex.MALE: [
                    IncludesExcludesSet(
                        members=MaleBiologicalSexIncludesExcludes,
                        excluded_set={
                            MaleBiologicalSexIncludesExcludes.UNKNOWN,
                        },
                    ),
                ],
                BiologicalSex.FEMALE: [
                    IncludesExcludesSet(
                        members=FemaleBiologicalSexIncludesExcludes,
                        excluded_set={
                            FemaleBiologicalSexIncludesExcludes.UNKNOWN,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BiologicalSex.MALE: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is male.",
                BiologicalSex.FEMALE: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is female.",
                BiologicalSex.UNKNOWN: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is not known.",
            },
        ),
    ],
)

criminal_case_filings = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.CASES_FILED,
    category=MetricCategory.POPULATIONS,
    display_name="Criminal Case Filings",
    description="The number of criminal cases filed with the court.",
    additional_description="If the same person is listed as the defendant in multiple cases, these cases are still counted separately if they were filed on different dates. If multiple charges or counts were filed against one person on the same date, with the expectation that they would be reviewed and filed together, these charges are combined to count as one case. If the charging document contains multiple defendants involved in a single incident, count each defendant as a single case.",
    unit=MetricUnit.CASES_FILED,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=CriminalCaseFilingsIncludesExcludes,
            excluded_set={
                CriminalCaseFilingsIncludesExcludes.VIOLATIONS,
                CriminalCaseFilingsIncludesExcludes.REVOCATIONS,
                CriminalCaseFilingsIncludesExcludes.REOPENED,
                CriminalCaseFilingsIncludesExcludes.TRANSFERRED_INTERNAL,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=CaseSeverityType,
            required=False,
            dimension_to_includes_excludes={
                CaseSeverityType.FELONY: [
                    IncludesExcludesSet(
                        members=FelonyCriminalCaseFilingsIncludesExcludes,
                    ),
                ],
                CaseSeverityType.MISDEMEANOR: [
                    IncludesExcludesSet(
                        members=MisdemeanorOrInfractionCriminalCaseFilingsIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                CaseSeverityType.FELONY: "The number of criminal cases filed with the court in which the leading charge was for a felony offense.",
                CaseSeverityType.MISDEMEANOR: "The number of criminal cases filed with the court in which the leading charge was for a misdemeanor offense.",
                CaseSeverityType.OTHER: "The number of criminal cases filed with the court in which the leading charge was not for a felony or misdemeanor or infraction offense.",
                CaseSeverityType.UNKNOWN: "The number of criminal cases filed with the court in which the leading charge was of unknown severity.",
            },
        )
    ],
)


cases_disposed = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.CASES_DISPOSED,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Cases Disposed",
    description="The number of criminal cases closed with the court.",
    additional_description="If the same person is listed as the defendant in multiple cases, these cases should be counted separately if they were disposed on different dates.",
    unit=MetricUnit.CASES,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=CasesDisposedIncludesExcludes,
            excluded_set={
                CasesDisposedIncludesExcludes.INACTIVE,
                CasesDisposedIncludesExcludes.PENDING,
            },
        )
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=DispositionType,
            required=False,
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
            dimension_to_description={
                DispositionType.DISMISSAL: "The number of criminal cases dismissed after filing and closed with the court.",
                DispositionType.PLEA: "The number of criminal cases resolved by plea and closed with the court.",
                DispositionType.TRIAL: "The number of criminal cases resolved by trial and closed with the court.",
                DispositionType.OTHER: "The number of criminal cases disposed closed with the court that were not dismissed, resolved by plea, or resolved at trial but disposed by another means.",
                DispositionType.UNKNOWN: "The number of criminal cases closed with the court for which the disposition method is unknown.",
            },
        )
    ],
)


new_offenses_while_on_pretrial_release = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.ARRESTS_ON_PRETRIAL_RELEASE,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="New Offenses While on Pretrial Release",
    description="The number of new arrests involving a person awaiting criminal trial in the community that are unrelated to their pending disposition.",
    additional_description="New offenses while on pretrial release are counted by the number of incidents that result in a new arrest. If a person has multiple charges under the same arrest, it should be counted as one new offense. If a person has three discrete incidents that result in arrests, that should count as three new offenses in this metric. If a person is arrested for a violation of their pretrial release, that is not considered a new offense in this metric.",
    unit=MetricUnit.ARRESTS_ON_PRETRIAL_RELEASE,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=NewOffensesWhileOnPretrialReleaseIncludesExcludes,
            excluded_set={
                NewOffensesWhileOnPretrialReleaseIncludesExcludes.AWAITING_DISPOSITION,
                NewOffensesWhileOnPretrialReleaseIncludesExcludes.TRANSFERRED,
            },
        ),
    ],
)
