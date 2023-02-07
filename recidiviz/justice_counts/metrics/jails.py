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
"""Defines all Justice Counts metrics for the Jail system."""
from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.jails import (
    ExpenseType,
    FundingType,
    PopulationType,
    ReadmissionType,
    ReleaseType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.jails import (
    ClinicalAndMedicalStaffIncludesExcludes,
    CommissaryAndFeesIncludesExcludes,
    ContractBedsExpensesIncludesExcludes,
    ContractBedsFundingIncludesExcludes,
    CountyOrMunicipalAppropriationIncludesExcludes,
    ExpensesIncludesExcludes,
    FacilitiesAndEquipmentIncludesExcludes,
    FundingIncludesExcludes,
    GrantsIncludesExcludes,
    HealthCareForPeopleWhoAreIncarceratedIncludesExcludes,
    ManagementAndOperationsStaffIncludesExcludes,
    PersonnelIncludesExcludes,
    ProgrammaticStaffIncludesExcludes,
    SecurityStaffIncludesExcludes,
    StaffIncludesExcludes,
    StateAppropriationIncludesExcludes,
    TrainingIncludesExcludes,
    UseOfForceIncidentsIncludesExcludes,
    VacantPositionsIncludesExcludes,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    Definition,
    IncludesExcludesSet,
    MetricCategory,
    MetricDefinition,
    YesNoContext,
)
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    ReportingFrequency,
    System,
)

residents = MetricDefinition(
    system=System.JAILS,
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
    system=System.JAILS,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    # TODO(#17577) implement multiple includes/excludes tables
    includes_excludes=IncludesExcludesSet(
        members=FundingIncludesExcludes,
        excluded_set={
            FundingIncludesExcludes.OPERATIONS_MAINTENANCE,
            FundingIncludesExcludes.JUVENILE,
            FundingIncludesExcludes.NON_JAIL_ACTIVITIES,
            FundingIncludesExcludes.LAW_ENFORCEMENT,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=False,
            dimension_to_description={
                FundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for the operation and maintenance of jail facilities and the care of people who are incarcerated in jail under the jurisdiction of the agency.",
                FundingType.COUNTY_MUNICIPAL: "The amount of funding counties or municipalities appropriated for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                FundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                FundingType.COMMISSARY_FEES: "The amount of funding the agency collected through sales and/or fees charged to people who are incarcerated under the jurisdiction of the agency or their visitors.",
                FundingType.CONTRACT_BEDS: "The amount of funding collected by the agency through contracts or per diem payment agreements to provide custody and care for people who are incarcerated under the jurisdiction of another agency.",
                FundingType.OTHER: "The amount of funding for the operation and maintenance of jail facilities and the care of people who are incarcerated that is not a state appropriation, a county or municipal appropriation, grant funding, commissary and fees, or contracted beds.",
                FundingType.UNKNOWN: "The amount of funding for the operation and maintenance of jail facilities and the care of people who are incarcerated for which the source is not known.",
            },
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATION: IncludesExcludesSet(
                    members=StateAppropriationIncludesExcludes,
                    excluded_set={
                        StateAppropriationIncludesExcludes.PROPOSED,
                        StateAppropriationIncludesExcludes.PRELIMINARY,
                        StateAppropriationIncludesExcludes.GRANTS,
                    },
                ),
                FundingType.COUNTY_MUNICIPAL: IncludesExcludesSet(
                    members=CountyOrMunicipalAppropriationIncludesExcludes,
                    excluded_set={
                        CountyOrMunicipalAppropriationIncludesExcludes.PROPOSED,
                        CountyOrMunicipalAppropriationIncludesExcludes.PRELIMINARY,
                    },
                ),
                FundingType.GRANTS: IncludesExcludesSet(
                    members=GrantsIncludesExcludes,
                ),
                FundingType.COMMISSARY_FEES: IncludesExcludesSet(
                    members=CommissaryAndFeesIncludesExcludes,
                ),
                FundingType.CONTRACT_BEDS: IncludesExcludesSet(
                    members=ContractBedsFundingIncludesExcludes,
                ),
            },
        )
    ],
)

expenses = MetricDefinition(
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    system=System.JAILS,
    description="The amount the agency spent for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    metric_type=MetricType.EXPENSES,
    display_name="Expenses",
    measurement_type=MeasurementType.DELTA,
    category=MetricCategory.CAPACITY_AND_COST,
    # TODO(#17577) Implement multiple includes/excludes tables
    includes_excludes=IncludesExcludesSet(
        members=ExpensesIncludesExcludes,
        excluded_set={
            ExpensesIncludesExcludes.OPERATIONS_MAINTENANCE,
            ExpensesIncludesExcludes.JUVENILE,
            ExpensesIncludesExcludes.NON_JAIL_ACTIVITIES,
            ExpensesIncludesExcludes.LAW_ENFORCEMENT,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            required=False,
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount the agency spent to employ personnel involved in the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.TRAINING: "The amount spent by the agency on training personnel involved in the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.FACILITIES: "The amount spent by the agency for the purchase and use of the physical plant and property owned and operated by the agency and equipment used to support maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.HEALTH_CARE: "The amount spent by the agency on medical care for people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.CONTRACT_BEDS: "The amount spent by the agency on contracts with other agencies to provide custody and care for people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.OTHER: "The amount spent by the agency on other costs relating to the operation and maintenance of jail facilities and the care of people who are incarcerated that are not personnel, training, facilities and equipment, health care for people who are incarcerated, or contract beds.",
                ExpenseType.UNKNOWN: "The amount spent by the agency on other costs relating to the operation and maintenance of jail facilities and the care of people who are incarcerated for a purpose that is not known.",
            },
            dimension_to_includes_excludes={
                ExpenseType.PERSONNEL: IncludesExcludesSet(
                    members=PersonnelIncludesExcludes,
                    excluded_set={
                        PersonnelIncludesExcludes.COMPANIES_CONTRACTED,
                    },
                ),
                ExpenseType.TRAINING: IncludesExcludesSet(
                    members=TrainingIncludesExcludes,
                    excluded_set={
                        TrainingIncludesExcludes.NO_COST_PROGRAMS,
                    },
                ),
                ExpenseType.FACILITIES: IncludesExcludesSet(
                    members=FacilitiesAndEquipmentIncludesExcludes,
                ),
                ExpenseType.HEALTH_CARE: IncludesExcludesSet(
                    members=HealthCareForPeopleWhoAreIncarceratedIncludesExcludes,
                    excluded_set={
                        HealthCareForPeopleWhoAreIncarceratedIncludesExcludes.TRANSPORT_COSTS,
                    },
                ),
                ExpenseType.CONTRACT_BEDS: IncludesExcludesSet(
                    members=ContractBedsExpensesIncludesExcludes,
                ),
            },
        )
    ],
)

total_staff = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent (FTE) positions budgeted and paid for by the agency for the operation and maintenance of the jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        members=StaffIncludesExcludes,
        excluded_set={
            StaffIncludesExcludes.VOLUNTEER,
            StaffIncludesExcludes.INTERN,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_includes_excludes={
                StaffType.SECURITY: IncludesExcludesSet(
                    members=SecurityStaffIncludesExcludes,
                    excluded_set={
                        SecurityStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.MANAGEMENT_AND_OPERATIONS: IncludesExcludesSet(
                    members=ManagementAndOperationsStaffIncludesExcludes,
                    excluded_set={
                        ManagementAndOperationsStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.CLINICAL_AND_MEDICAL: IncludesExcludesSet(
                    members=ClinicalAndMedicalStaffIncludesExcludes,
                    excluded_set={
                        ClinicalAndMedicalStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.PROGRAMMATIC: IncludesExcludesSet(
                    members=ProgrammaticStaffIncludesExcludes,
                    excluded_set={
                        ProgrammaticStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.VACANT: IncludesExcludesSet(
                    members=VacantPositionsIncludesExcludes,
                    excluded_set={
                        VacantPositionsIncludesExcludes.FILLED,
                    },
                ),
            },
            dimension_to_description={
                StaffType.SECURITY: "The number of full-time equivalent positions that work directly with people who are incarcerated and are responsible for their custody, supervision, and monitoring.",
                StaffType.MANAGEMENT_AND_OPERATIONS: "The number of full-time equivalent positions that do not work directly with people who are incarcerated but support the day-to-day operations of the agency.",
                StaffType.CLINICAL_AND_MEDICAL: "The number of full-time equivalent positions that work directly with people who are incarcerated and are responsible for their health.",
                StaffType.PROGRAMMATIC: "The number of full-time equivalent positions that provide services and programming to people who are incarcerated but are not medical or clinical staff.",
                StaffType.OTHER: "The number of full-time equivalent positions dedicated to the operation and maintenance of jail facilities under the jurisdiction of the agency that are not security staff, management and operations staff, clinical and medical staff, or programmatic staff.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions dedicated to the operation and maintenance of jail facilities under the jurisdiction of the agency that are of an unknown type.",
                StaffType.VACANT: "The number of full-time equivalent positions dedicated to operation and maintenance of the jail facilities and the care of people who are incarcerated under the jurisdiction of the agency of any type that are budgeted but not currently filled.",
            },
        )
    ],
)

readmissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.READMISSIONS,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Readmissions",
    description="Measures the number of individuals admitted who had at least one other jail admission within the prior 12 months.",
    reporting_note="You may only be able to identify if an individual was admitted to your same facility within the last year.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[],
    aggregated_dimensions=[
        AggregatedDimension(dimension=ReadmissionType, required=False)
    ],
)

admissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Admissions",
    description="Measures the number of new admissions to your jail system.",
    reporting_note="Report individuals in the most serious category (new sentence > violation > hold).",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_ADMISSION,
            value_type=ValueType.TEXT,
            label="Please provide your agency's definition of admission.",
            required=True,
        ),
        Context(
            key=ContextKey.INCLUDES_VIOLATED_CONDITIONS,
            value_type=ValueType.MULTIPLE_CHOICE,
            label="Are the individuals admitted for violation of conditions counted within the total population?",
            required=False,
            multiple_choice_options=YesNoContext,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=PopulationType, required=False)
    ],
)

average_daily_population = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Average Daily Population",
    description="Measures the average daily population of individuals held in jail custody.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[],
    aggregated_dimensions=[
        AggregatedDimension(dimension=PopulationType, required=False),
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(dimension=GenderRestricted, required=True),
    ],
)

releases = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Releases",
    description="Measures the number of new releases from your jail system.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[AggregatedDimension(dimension=ReleaseType, required=False)],
)

staff_use_of_force_incidents = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Use of Force Incidents",
    description="The number of incidents in which agency staff use physical force to gain compliance from or control of a person who is under the agency’s jurisdiction.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        members=UseOfForceIncidentsIncludesExcludes,
        excluded_set={
            UseOfForceIncidentsIncludesExcludes.ROUTINE,
        },
    ),
)

grievances_upheld = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.GRIEVANCES_UPHELD,
    category=MetricCategory.FAIRNESS,
    display_name="Grievances Upheld",
    definitions=[
        Definition(
            term="Grievance",
            definition="A complaint or question filed with your institution by an individual incarcerated regarding their experience, with procedures, treatment, or interaction with officers.",
        )
    ],
    description="Measures the number of grievances filed with your institution that were upheld.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
)
