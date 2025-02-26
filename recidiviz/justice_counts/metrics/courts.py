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

from recidiviz.justice_counts.dimensions.common import ExpenseType
from recidiviz.justice_counts.dimensions.courts import (
    CaseSeverityType,
    FundingType,
    ReleaseType,
    SentenceType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.common import (
    CountyOrMunicipalAppropriationIncludesExcludes,
    GrantsIncludesExcludes,
    StaffIncludesExcludes,
    StateAppropriationIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.courts import (
    CasesOverturnedOnAppealIncludesExcludes,
    CommunitySupervisionOnlySentencesIncludesExcludes,
    CriminalCaseFilingsIncludesExcludes,
    ExpensesFacilitiesAndEquipmentIncludesExcludes,
    ExpensesIncludesExcludes,
    ExpensesPersonnelIncludesExcludes,
    ExpensesTrainingIncludesExcludes,
    FelonyCriminalCaseFilingsIncludesExcludes,
    FinesOrFeesOnlySentencesIncludesExcludes,
    FundingIncludesExcludes,
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
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    System,
)

residents = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.RESIDENTS,
    category=MetricCategory.POPULATIONS,
    display_name="Jurisdiction Residents",
    description="Measures the number of residents in your agency's jurisdiction.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY, ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(dimension=GenderRestricted, required=True),
    ],
    disabled=True,
)

funding = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of the court system to process criminal cases.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    # TODO(#17577) implement multiple includes/excludes tables
    includes_excludes=[
        IncludesExcludesSet(
            members=FundingIncludesExcludes,
            excluded_set={
                FundingIncludesExcludes.SUPERVISION_OPERATIONS,
                FundingIncludesExcludes.JUVENILE,
                FundingIncludesExcludes.NON_COURT_FUNCTIONS,
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
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    # TODO(#17577) implement multiple includes/excludes tables
    includes_excludes=[
        IncludesExcludesSet(
            members=ExpensesIncludesExcludes,
            excluded_set={
                ExpensesIncludesExcludes.COMMUNITY_SUPERVISION_OPERATIONS,
                ExpensesIncludesExcludes.JUVENILE,
                ExpensesIncludesExcludes.NON_COURT_FUNCTIONS,
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
        # TODO(#17579) implement yes/no tables
        AggregatedDimension(dimension=RaceAndEthnicity, required=False),
        # TODO(#18071) reuse global includes/excludes
        # TODO(#17579) implement yes/no tables
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


new_offenses_while_on_pretrial_release = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.ARRESTS_ON_PRETRIAL_RELEASE,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="New Offenses While on Pretrial Release",
    description="The number of new arrests involving a person awaiting criminal trial in the community that are unrelated to their pending disposition.",
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

cases_overturned = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.CASES_OVERTURNED_ON_APPEAL,
    category=MetricCategory.FAIRNESS,
    display_name="Cases Overturned on Appeal",
    description="The number of criminal cases that were overturned on appeal.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=CasesOverturnedOnAppealIncludesExcludes,
            excluded_set={
                CasesOverturnedOnAppealIncludesExcludes.INTERLOCUTORY_APPEAL,
            },
        ),
    ],
)
