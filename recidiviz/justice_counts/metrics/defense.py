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
"""Defines all Justice Counts metrics for Defense."""

from recidiviz.justice_counts.dimensions.common import CaseSeverityType, DispositionType
from recidiviz.justice_counts.dimensions.defense import (
    CaseAppointedSeverityType,
    ExpenseType,
    FundingType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.includes_excludes.common import (
    CaseloadNumeratorIncludesExcludes,
    CasesDismissedIncludesExcludes,
    CasesResolvedAtTrialIncludesExcludes,
    CasesResolvedByPleaIncludesExcludes,
    CountyOrMunicipalAppropriationIncludesExcludes,
    FacilitiesAndEquipmentExpensesIncludesExcludes,
    FelonyCaseloadNumeratorIncludesExcludes,
    FelonyCasesIncludesExcludes,
    GrantsIncludesExcludes,
    MisdemeanorCaseloadNumeratorIncludesExcludes,
    MisdemeanorCasesIncludesExcludes,
    MixedCaseloadNumeratorIncludesExcludes,
    StaffIncludesExcludes,
    StateAppropriationIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.defense import (
    DefenseAdministrativeStaffIncludesExcludes,
    DefenseCaseloadDenominatorIncludesExcludes,
    DefenseCasesAppointedCounselIncludesExcludes,
    DefenseCasesDisposedIncludesExcludes,
    DefenseComplaintsIncludesExcludes,
    DefenseExpensesTimeframeIncludesExcludes,
    DefenseExpensesTypeIncludesExcludes,
    DefenseFelonyCaseloadDenominatorIncludesExcludes,
    DefenseFundingPurposeIncludesExcludes,
    DefenseFundingTimeframeIncludesExcludes,
    DefenseInvestigativeStaffIncludesExcludes,
    DefenseLegalStaffIncludesExcludes,
    DefenseMisdemeanorCaseloadDenominatorIncludesExcludes,
    DefenseMixedCaseloadDenominatorIncludesExcludes,
    DefensePersonnelExpensesIncludesExcludes,
    DefenseTrainingExpensesIncludesExcludes,
    DefenseVacantStaffIncludesExcludes,
    FeesFundingIncludesExcludes,
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
)
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    ReportingFrequency,
    System,
)

funding = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of defense providers and criminal public defense services.",
    measurement_type=MeasurementType.INSTANT,
    includes_excludes=[
        IncludesExcludesSet(
            members=DefenseFundingTimeframeIncludesExcludes,
            description="Funding timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=DefenseFundingPurposeIncludesExcludes,
            description="Funding purpose",
            excluded_set={DefenseFundingPurposeIncludesExcludes.NON_CRIMINAL_CASES},
        ),
    ],
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=False,
            dimension_to_description={
                FundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for the operation and maintenance of defense providers and criminal public defense services.",
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATION: "The amount of funding appropriated by counties or municipalities for the operation and maintenance of defense providers and criminal public defense services.",
                FundingType.GRANTS: "The amount of funding derived by the provider through grants and awards to be used for the operation and maintenance of defense providers and criminal public defense services.",
                FundingType.FEES: "The amount of funding earned by the provider through fees collected from people who are criminal public defense clients of the provider.",
                FundingType.OTHER: "The amount of funding to be used for the operation and maintenance of defense providers and criminal public defense services that is not appropriations from the state, appropriations from the county or city, grants, or fees.",
                FundingType.UNKNOWN: "The amount of funding to be used for the operation and maintenance of defense providers and criminal public defense services for which the source is not known.",
            },
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
                FundingType.FEES: [
                    IncludesExcludesSet(
                        members=FeesFundingIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)


expenses = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.EXPENSES,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Expenses",
    description="The amount spent for the operation and maintenance of defense providers and criminal public defense services.",
    measurement_type=MeasurementType.INSTANT,
    includes_excludes=[
        IncludesExcludesSet(
            members=DefenseExpensesTimeframeIncludesExcludes,
            description="Expenses timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=DefenseExpensesTypeIncludesExcludes,
            description="Expense type",
            excluded_set={DefenseExpensesTypeIncludesExcludes.NON_CRIMINAL_CASES},
        ),
    ],
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            required=False,
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount spent by the office to employ personnel involved in the operation and maintenance of defense providers and criminal public defense services.",
                ExpenseType.TRAINING: "The amount spent on the training of personnel involved in the operation and maintenance of criminal public defense providers and the representation of people who are clients of those providers, including any associated expenses, such as registration fees and travel costs.",
                ExpenseType.FACILITIES_AND_EQUIPMENT: "The amount spent for the purchase and use of the physical plant and property owned and operated by the provider for criminal defense services.",
                ExpenseType.OTHER: "The amount spent on other costs relating the operation and maintenance of criminal public defense providers and the representation of people who are clients of those providers that are not personnel, training, or facilities and equipment expenses.",
                ExpenseType.UNKNOWN: "The amount spent on other costs relating the operation and maintenance of criminal defense providers and the representation of people who are clients of those providers that are for an unknown purpose.",
            },
            dimension_to_includes_excludes={
                ExpenseType.PERSONNEL: [
                    IncludesExcludesSet(
                        members=DefensePersonnelExpensesIncludesExcludes,
                        excluded_set={
                            DefensePersonnelExpensesIncludesExcludes.COMPANIES_CONTRACTED
                        },
                    ),
                ],
                ExpenseType.TRAINING: [
                    IncludesExcludesSet(
                        members=DefenseTrainingExpensesIncludesExcludes,
                        excluded_set={
                            DefenseTrainingExpensesIncludesExcludes.FREE_COURSES
                        },
                    ),
                ],
                ExpenseType.FACILITIES_AND_EQUIPMENT: [
                    IncludesExcludesSet(
                        members=FacilitiesAndEquipmentExpensesIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)

staff = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the provider for criminal defense services.",
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
            dimension_to_description={
                StaffType.ADMINISTRATIVE: "The number of full-time equivalent positions that work to support the day-to-day organization and logistics of the provider for criminal defense services.",
                StaffType.LEGAL: "The number of full-time equivalent positions that are responsible for the criminal legal representation of clients or support that work through the provision of legal services.",
                StaffType.INVESTIGATIVE: "The number of full-time equivalent positions that work to obtain evidence in support of the work of legal staff for criminal defense services.",
                StaffType.OTHER: "The number of full-time equivalent positions that provide or support criminal defense services that are not legal staff, administrative staff, or investigative staff.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions that provide or support criminal defense services of an unknown type.",
                StaffType.VACANT: "The number of full-time equivalent positions that provide or support criminal defense services of any type that are budgeted but not currently filled.",
            },
            dimension_to_includes_excludes={
                StaffType.LEGAL: [
                    IncludesExcludesSet(
                        members=DefenseLegalStaffIncludesExcludes,
                        excluded_set={
                            DefenseLegalStaffIncludesExcludes.CURRENTLY_VACANT
                        },
                    ),
                ],
                StaffType.ADMINISTRATIVE: [
                    IncludesExcludesSet(
                        members=DefenseAdministrativeStaffIncludesExcludes,
                        excluded_set={
                            DefenseAdministrativeStaffIncludesExcludes.CURRENTLY_VACANT
                        },
                    ),
                ],
                StaffType.INVESTIGATIVE: [
                    IncludesExcludesSet(
                        members=DefenseInvestigativeStaffIncludesExcludes,
                        excluded_set={
                            DefenseInvestigativeStaffIncludesExcludes.CURRENTLY_VACANT
                        },
                    ),
                ],
                StaffType.VACANT: [
                    IncludesExcludesSet(
                        members=DefenseVacantStaffIncludesExcludes,
                        excluded_set={DefenseVacantStaffIncludesExcludes.FILLED},
                    ),
                ],
            },
        )
    ],
)

cases_appointed_counsel = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.CASES_APPOINTED_COUNSEL,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Appointed Counsel",
    description="The number of criminal cases opened by the provider.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=DefenseCasesAppointedCounselIncludesExcludes,
            excluded_set={
                DefenseCasesAppointedCounselIncludesExcludes.INACTIVE,
                DefenseCasesAppointedCounselIncludesExcludes.TRANSFERRED,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            # TODO(#18071) Replace this with reference to Global Includes/Excludes once those are implemented
            dimension=CaseAppointedSeverityType,
            required=False,
            dimension_to_description={
                CaseAppointedSeverityType.MISDEMEANOR: "The number of criminal cases appointed for representation by attorneys employed by the provider in which the leading charge was for a misdemeanor offense.",
                CaseAppointedSeverityType.FELONY: "The number of criminal cases appointed for representation by attorneys employed by the provider in which the leading charge was for a felony offense.",
                CaseAppointedSeverityType.OTHER: "The number of criminal cases appointed for representation by attorneys employed by the provider in which the leading charge was not for a felony or misdemeanor.",
                CaseAppointedSeverityType.UNKNOWN: "The number of criminal cases appointed for representation by attorneys employed by the provider in which the leading charge was of unknown severity.",
            },
            dimension_to_includes_excludes={
                CaseAppointedSeverityType.FELONY: [
                    IncludesExcludesSet(
                        members=FelonyCasesIncludesExcludes,
                        excluded_set={FelonyCasesIncludesExcludes.MISDEMEANOR},
                    ),
                ],
                CaseAppointedSeverityType.MISDEMEANOR: [
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

caseload_numerator = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.CASELOADS_PEOPLE,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Open Cases",
    description="The number of people with open criminal cases carried by the provider (used as the numerator in the calculation of the caseload metric).",
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
                CaseSeverityType.MIXED: "The  number of people with open felony and misdemeanor cases.",
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
    system=System.DEFENSE,
    metric_type=MetricType.CASELOADS_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff with Caseload",
    description="The number of legal staff carrying a criminal caseload (used as the denominator in the calculation of the caseload metric).",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=DefenseCaseloadDenominatorIncludesExcludes,
            excluded_set={
                DefenseCaseloadDenominatorIncludesExcludes.ON_LEAVE,
            },
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
                        members=DefenseFelonyCaseloadDenominatorIncludesExcludes,
                        excluded_set={
                            DefenseFelonyCaseloadDenominatorIncludesExcludes.MIXED,
                        },
                    ),
                ],
                CaseSeverityType.MISDEMEANOR: [
                    IncludesExcludesSet(
                        members=DefenseMisdemeanorCaseloadDenominatorIncludesExcludes,
                        excluded_set={
                            DefenseMisdemeanorCaseloadDenominatorIncludesExcludes.MIXED,
                        },
                    ),
                ],
                CaseSeverityType.MIXED: [
                    IncludesExcludesSet(
                        members=DefenseMixedCaseloadDenominatorIncludesExcludes,
                        excluded_set={
                            DefenseMixedCaseloadDenominatorIncludesExcludes.FELONY_ONLY,
                            DefenseMixedCaseloadDenominatorIncludesExcludes.MISDEMEANOR_ONLY,
                        },
                    ),
                ],
            },
        )
    ],
)

cases_disposed = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.CASES_DISPOSED,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Cases Disposed",
    description="The number of criminal cases for which representation by the provider ended during the time period.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=DefenseCasesDisposedIncludesExcludes,
            excluded_set={
                DefenseCasesDisposedIncludesExcludes.INACTIVE,
                DefenseCasesDisposedIncludesExcludes.PENDING,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=DispositionType,
            required=False,
            dimension_to_description={
                DispositionType.DISMISSAL: "The number of criminal cases dismissed after filing and closed by the provider.",
                DispositionType.PLEA: "The number of criminal cases resolved by plea and closed by the provider.",
                DispositionType.TRIAL: "The number of criminal cases resolved by trial.",
                DispositionType.OTHER: "The number of criminal cases disposed by the provider that were not dismissed, resolved by plea, or resolved at trial but disposed by another means.",
                DispositionType.UNKNOWN: "The number of criminal cases disposed by the provider for which the disposition method is unknown.",
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
        ),
        AggregatedDimension(
            # TODO(#18071)
            dimension=BiologicalSex,
            required=False,
            dimension_to_description={
                BiologicalSex.MALE: "The number of criminal cases disposed in which the defendant’s biological sex is male.",
                BiologicalSex.FEMALE: "The number of criminal cases disposed in which the defendant’s biological sex is female.",
                BiologicalSex.UNKNOWN: "The number of criminal cases disposed in which the defendant’s biological sex is unknown.",
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
        AggregatedDimension(dimension=RaceAndEthnicity, required=False),
    ],
)

client_complaints_sustained = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.COMPLAINTS_SUSTAINED,
    category=MetricCategory.FAIRNESS,
    display_name="Client Complaints Sustained",
    description="The number of formal, written complaints made against criminal defense counsel that were subsequently sustained by the disciplinary board.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=DefenseComplaintsIncludesExcludes,
            excluded_set={
                DefenseComplaintsIncludesExcludes.DUPLICATE,
                DefenseComplaintsIncludesExcludes.INFORMAL,
                DefenseComplaintsIncludesExcludes.PENDING,
                DefenseComplaintsIncludesExcludes.UNSUBSTANTIATED,
            },
        ),
    ],
)
