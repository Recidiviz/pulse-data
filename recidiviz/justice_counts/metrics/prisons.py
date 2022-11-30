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
"""Defines all Justice Counts metrics for the Prison system."""

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.jails_and_prisons import (
    PrisonsExpenseType,
    PrisonsOffenseType,
    PrisonsReadmissionType,
    PrisonsReleaseType,
    PrisonsStaffType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.person import GenderIncludesExcludes
from recidiviz.justice_counts.includes_excludes.prisons import (
    PrisonAdmissionsIncludesExcludes,
    PrisonAverageDailyPopulationIncludesExcludes,
    PrisonBudgetIncludesExcludes,
    PrisonClinicalStaffIncludesExcludes,
    PrisonDrugOffenseIncludesExcludes,
    PrisonExpensesContractBedsIncludesExcludes,
    PrisonExpensesFacilitiesAndEquipmentIncludesExcludes,
    PrisonExpensesHealthCareIncludesExcludes,
    PrisonExpensesIncludesExcludes,
    PrisonExpensesPersonnelIncludesExcludes,
    PrisonExpensesTrainingIncludesExcludes,
    PrisonGrievancesIncludesExcludes,
    PrisonManagementAndOperationsStaffIncludesExcludes,
    PrisonPersonOffenseIncludesExcludes,
    PrisonProgrammaticStaffIncludesExcludes,
    PrisonPropertyOffenseIncludesExcludes,
    PrisonPublicOrderOffenseIncludesExcludes,
    PrisonReadmissionsIncludesExcludes,
    PrisonReadmissionsNewCommitmentIncludesExcludes,
    PrisonReadmissionsParoleIncludesExcludes,
    PrisonReadmissionsProbationIncludesExcludes,
    PrisonReleasesDeathIncludesExcludes,
    PrisonReleasesIncludesExcludes,
    PrisonReleasesNoAdditionalCorrectionalControlIncludesExcludes,
    PrisonReleasesToParoleIncludesExcludes,
    PrisonReleasesToProbationIncludesExcludes,
    PrisonSecurityStaffIncludesExcludes,
    PrisonStaffIncludesExcludes,
    PrisonUseOfForceIncludesExcludes,
    VacantPrisonStaffIncludesExcludes,
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
    system=System.PRISONS,
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

annual_budget = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of your state correctional institutions.",
    includes_excludes=IncludesExcludesSet(
        members=PrisonBudgetIncludesExcludes,
        excluded_set={
            PrisonBudgetIncludesExcludes.BIENNIUM_FUNDING,
            PrisonBudgetIncludesExcludes.MULTI_YEAR_APPROPRIATIONS,
            PrisonBudgetIncludesExcludes.JAILS,
            PrisonBudgetIncludesExcludes.COMMUNITY_SUPERVISION,
            PrisonBudgetIncludesExcludes.JUVENILE_CORRECTIONAL_FACILITIES,
        },
    ),
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="DOCs should only report on their correctional institution budget.",
    specified_contexts=[
        Context(
            key=ContextKey.PRIMARY_FUNDING_SOURCE,
            value_type=ValueType.TEXT,
            label="Please describe your primary budget source.",
            required=False,
        )
    ],
)

total_staff = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the agency for the operation and maintenance of the prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    definitions=[
        Definition(
            term="Full-time staff",
            definition="Number of people employed in a full-time (0.9+) capacity.",
        )
    ],
    includes_excludes=IncludesExcludesSet(
        members=PrisonStaffIncludesExcludes,
        excluded_set={
            PrisonStaffIncludesExcludes.VOLUNTEER,
            PrisonStaffIncludesExcludes.INTERN,
        },
    ),
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="DOCs should only report on their correctional institution staff.",
    specified_contexts=[
        Context(
            key=ContextKey.INCLUDES_PROGRAMMATIC_STAFF,
            value_type=ValueType.MULTIPLE_CHOICE,
            label="Does your count include programmatic staff?",
            required=True,
            multiple_choice_options=YesNoContext,
        ),
        Context(
            key=ContextKey.INCLUDES_MEDICAL_STAFF,
            value_type=ValueType.MULTIPLE_CHOICE,
            label="Does your count include medical staff?",
            required=True,
            multiple_choice_options=YesNoContext,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsStaffType,
            required=False,
            dimension_to_includes_excludes={
                PrisonsStaffType.SECURITY: IncludesExcludesSet(
                    members=PrisonSecurityStaffIncludesExcludes,
                    excluded_set={
                        PrisonSecurityStaffIncludesExcludes.VACANT,
                    },
                ),
                PrisonsStaffType.MANAGEMENT_AND_OPERATIONS: IncludesExcludesSet(
                    members=PrisonManagementAndOperationsStaffIncludesExcludes,
                    excluded_set={
                        PrisonManagementAndOperationsStaffIncludesExcludes.VACANT,
                    },
                ),
                PrisonsStaffType.CLINICAL_OR_MEDICAL: IncludesExcludesSet(
                    members=PrisonClinicalStaffIncludesExcludes,
                    excluded_set={
                        PrisonClinicalStaffIncludesExcludes.VACANT,
                    },
                ),
                PrisonsStaffType.PROGRAMMATIC: IncludesExcludesSet(
                    members=PrisonProgrammaticStaffIncludesExcludes,
                    excluded_set={
                        PrisonProgrammaticStaffIncludesExcludes.VACANT,
                        PrisonProgrammaticStaffIncludesExcludes.VOLUNTEER,
                    },
                ),
                PrisonsStaffType.VACANT: IncludesExcludesSet(
                    members=VacantPrisonStaffIncludesExcludes,
                    excluded_set={
                        VacantPrisonStaffIncludesExcludes.FILLED,
                    },
                ),
            },
        )
    ],
)

expenses = MetricDefinition(
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    system=System.PRISONS,
    description="The amount spent by the agency for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    metric_type=MetricType.EXPENSES,
    display_name="Expenses",
    specified_contexts=[],
    measurement_type=MeasurementType.DELTA,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    includes_excludes=IncludesExcludesSet(
        members=PrisonExpensesIncludesExcludes,
        excluded_set={
            PrisonExpensesIncludesExcludes.BIENNNIUM_EXPENSES,
            PrisonExpensesIncludesExcludes.MULTI_YEAR,
            PrisonExpensesIncludesExcludes.JAIL_FACILITY,
            PrisonExpensesIncludesExcludes.JUVENILE_JAIL,
            PrisonExpensesIncludesExcludes.LAW_ENFORCEMENT,
            PrisonExpensesIncludesExcludes.NON_PRISON_ACTIVITIES,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsExpenseType,
            required=False,
            dimension_to_includes_excludes={
                PrisonsExpenseType.PERSONNEL: IncludesExcludesSet(
                    members=PrisonExpensesPersonnelIncludesExcludes,
                    excluded_set={PrisonExpensesPersonnelIncludesExcludes.CONTRACTORS},
                ),
                PrisonsExpenseType.TRAINING: IncludesExcludesSet(
                    members=PrisonExpensesTrainingIncludesExcludes,
                    excluded_set={PrisonExpensesTrainingIncludesExcludes.FREE_PROGRAMS},
                ),
                PrisonsExpenseType.FACILITIES_AND_EQUIPMENT: IncludesExcludesSet(
                    members=PrisonExpensesFacilitiesAndEquipmentIncludesExcludes,
                ),
                PrisonsExpenseType.HEALTH_CARE: IncludesExcludesSet(
                    members=PrisonExpensesHealthCareIncludesExcludes,
                    excluded_set={
                        PrisonExpensesHealthCareIncludesExcludes.TRANSPORTATION
                    },
                ),
                PrisonsExpenseType.CONTRACT_BEDS: IncludesExcludesSet(
                    members=PrisonExpensesContractBedsIncludesExcludes,
                ),
            },
        )
    ],
)

readmissions = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.READMISSIONS,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Readmissions",
    description="Measures the number of individuals admitted who had at least one other prison admission within the prior 12 months.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Exclude re-entry after a temporary exit (escape, work release, appointment, etc.).",
    specified_contexts=[],
    includes_excludes=IncludesExcludesSet(
        members=PrisonReadmissionsIncludesExcludes,
        excluded_set={
            PrisonReadmissionsIncludesExcludes.TEMP_ABSENCE,
            PrisonReadmissionsIncludesExcludes.TRANSFERRED_IN_STATE,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsReadmissionType,
            required=False,
            dimension_to_includes_excludes={
                PrisonsReadmissionType.NEW_ADMISSION: IncludesExcludesSet(
                    members=PrisonReadmissionsNewCommitmentIncludesExcludes,
                    excluded_set={
                        PrisonReadmissionsNewCommitmentIncludesExcludes.OTHER
                    },
                ),
                PrisonsReadmissionType.RETURN_FROM_PROBATION: IncludesExcludesSet(
                    members=PrisonReadmissionsProbationIncludesExcludes,
                    excluded_set={PrisonReadmissionsProbationIncludesExcludes.OTHER},
                ),
                PrisonsReadmissionType.RETURN_FROM_PAROLE: IncludesExcludesSet(
                    members=PrisonReadmissionsParoleIncludesExcludes,
                    excluded_set={PrisonReadmissionsParoleIncludesExcludes.OTHER},
                ),
            },
        )
    ],
)

admissions = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Admissions",
    description="Measures the number of new admissions to your state correctional system.",
    measurement_type=MeasurementType.DELTA,
    reporting_note="Report individuals in the most serious category (new sentence > violation > hold).",
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=PrisonAdmissionsIncludesExcludes,
        excluded_set={
            PrisonAdmissionsIncludesExcludes.RETURNING_FROM_ABSENCE,
            PrisonAdmissionsIncludesExcludes.TRANSFERRED_BETWEEN_FACILITIES,
        },
    ),
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
            label="Does your count include individuals admitted for violation of conditions?",
            multiple_choice_options=YesNoContext,
            required=False,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsOffenseType,
            required=False,
            display_name="Prison Offense Type",
            dimension_to_includes_excludes={
                PrisonsOffenseType.PERSON: IncludesExcludesSet(
                    members=PrisonPersonOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPersonOffenseIncludesExcludes.ARSON,
                        PrisonPersonOffenseIncludesExcludes.HARASSMENT,
                        PrisonPersonOffenseIncludesExcludes.OTHER,
                    },
                ),
                PrisonsOffenseType.PROPERTY: IncludesExcludesSet(
                    members=PrisonPropertyOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPropertyOffenseIncludesExcludes.OTHER,
                    },
                ),
                PrisonsOffenseType.DRUG: IncludesExcludesSet(
                    members=PrisonDrugOffenseIncludesExcludes,
                    excluded_set={
                        PrisonDrugOffenseIncludesExcludes.OTHER,
                    },
                ),
                PrisonsOffenseType.PUBLIC_ORDER: IncludesExcludesSet(
                    members=PrisonPublicOrderOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPublicOrderOffenseIncludesExcludes.OTHER,
                    },
                ),
            },
        )
    ],
)

average_daily_population = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Average Daily Population",
    description="Measures the average daily population of individuals held in your state corrections system.",
    includes_excludes=IncludesExcludesSet(
        members=PrisonAverageDailyPopulationIncludesExcludes,
        excluded_set={
            PrisonAverageDailyPopulationIncludesExcludes.AWOL,
            PrisonAverageDailyPopulationIncludesExcludes.HOUSED_FOR_OTHER_AGENCIES,
        },
    ),
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Calculate the average against a 30-day month. Report individuals in the most serious category (new sentence > violation > hold).",
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(
            dimension=GenderRestricted,
            required=True,
            dimension_to_includes_excludes={
                GenderRestricted.MALE: IncludesExcludesSet(
                    members=GenderIncludesExcludes,
                    excluded_set={
                        GenderIncludesExcludes.FEMALE,
                        GenderIncludesExcludes.OTHER,
                        GenderIncludesExcludes.UNKNOWN,
                    },
                ),
                GenderRestricted.FEMALE: IncludesExcludesSet(
                    members=GenderIncludesExcludes,
                    excluded_set={
                        GenderIncludesExcludes.MALE,
                        GenderIncludesExcludes.OTHER,
                        GenderIncludesExcludes.UNKNOWN,
                    },
                ),
            },
        ),
        AggregatedDimension(
            dimension=PrisonsOffenseType,
            required=False,
            display_name="Prison Offense Type",
            dimension_to_includes_excludes={
                PrisonsOffenseType.PERSON: IncludesExcludesSet(
                    members=PrisonPersonOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPersonOffenseIncludesExcludes.ARSON,
                        PrisonPersonOffenseIncludesExcludes.HARASSMENT,
                        PrisonPersonOffenseIncludesExcludes.OTHER,
                    },
                ),
                PrisonsOffenseType.PROPERTY: IncludesExcludesSet(
                    members=PrisonPropertyOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPropertyOffenseIncludesExcludes.OTHER,
                    },
                ),
                PrisonsOffenseType.DRUG: IncludesExcludesSet(
                    members=PrisonDrugOffenseIncludesExcludes,
                    excluded_set={
                        PrisonDrugOffenseIncludesExcludes.OTHER,
                    },
                ),
                PrisonsOffenseType.PUBLIC_ORDER: IncludesExcludesSet(
                    members=PrisonPublicOrderOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPublicOrderOffenseIncludesExcludes.OTHER,
                    },
                ),
            },
        ),
    ],
)

releases = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Releases",
    description="Measures the number of releases from your state corrections system.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Exclude temporary release (work release, appointment, court hearing, etc.).",
    includes_excludes=IncludesExcludesSet(
        members=PrisonReleasesIncludesExcludes,
        excluded_set={
            PrisonReleasesIncludesExcludes.TEMP_EXIT,
            PrisonReleasesIncludesExcludes.TEMP_TRANSFER,
            PrisonReleasesIncludesExcludes.TRANSFERRED_IN,
        },
    ),
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_SUPERVISION,
            value_type=ValueType.TEXT,
            label="Please provide your agency's definition of supervision (i.e. probation, parole, or both).",
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsReleaseType,
            required=False,
            dimension_to_includes_excludes={
                PrisonsReleaseType.TO_PAROLE_SUPERVISION: IncludesExcludesSet(
                    members=PrisonReleasesToParoleIncludesExcludes,
                    excluded_set={PrisonReleasesToParoleIncludesExcludes.OTHER},
                ),
                PrisonsReleaseType.TO_PROBATION_SUPERVISION: IncludesExcludesSet(
                    members=PrisonReleasesToProbationIncludesExcludes,
                    excluded_set={PrisonReleasesToProbationIncludesExcludes.OTHER},
                ),
                PrisonsReleaseType.SENTENCE_COMPLETION: IncludesExcludesSet(
                    members=PrisonReleasesNoAdditionalCorrectionalControlIncludesExcludes,
                    excluded_set={
                        PrisonReleasesNoAdditionalCorrectionalControlIncludesExcludes.OTHER
                    },
                ),
                PrisonsReleaseType.DEATH: IncludesExcludesSet(
                    members=PrisonReleasesDeathIncludesExcludes,
                    excluded_set={PrisonReleasesDeathIncludesExcludes.OTHER},
                ),
            },
        )
    ],
)

staff_use_of_force_incidents = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Use of Force Incidents",
    description="The number of incidents in which agency staff use physical force to gain compliance from or control of a person who is under the agency’s jurisdiction.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Select the most serious type of force used per incident.",
    definitions=[
        Definition(
            term="Use of force incident",
            definition="An event in which an officer uses force towards or in the vicinity of an individual incarcerated. The AJA focuses on uses of force resulting in injury or a discharge of a weapon.  Count all uses of force occurring during the same event as one incident.",
        )
    ],
    includes_excludes=IncludesExcludesSet(
        members=PrisonUseOfForceIncludesExcludes,
        excluded_set={PrisonUseOfForceIncludesExcludes.ROUTINE},
    ),
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_USE_OF_FORCE,
            value_type=ValueType.TEXT,
            label='Please provide your agency\'s definition of "use of force".',
            required=True,
        ),
    ],
)

grievances_upheld = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.GRIEVANCES_UPHELD,
    category=MetricCategory.FAIRNESS,
    display_name="Grievances Upheld",
    description="Measures the number of grievances filed with the institution that were upheld.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    definitions=[
        Definition(
            term="Grievance",
            definition="A complaint or question filed with the institution by an individual incarcerated regarding their experience, with procedures, treatment, or interaction with officers.",
        )
    ],
    includes_excludes=IncludesExcludesSet(
        members=PrisonGrievancesIncludesExcludes,
        excluded_set={
            PrisonGrievancesIncludesExcludes.DUPLICATE,
            PrisonGrievancesIncludesExcludes.PENDING_RESOLUTION,
            PrisonGrievancesIncludesExcludes.INFORMAL,
        },
    ),
)
