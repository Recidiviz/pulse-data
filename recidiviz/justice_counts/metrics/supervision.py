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
"""Defines all Justice Counts metrics for Supervision."""

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.supervision import (
    NewOffenseType,
    SupervisionCaseType,
    SupervisionIndividualType,
    SupervisionStaffType,
    SupervisionTerminationType,
    SupervisionViolationType,
)
from recidiviz.justice_counts.includes_excludes.supervision import (
    SupervisionClinicalMedicalStaffIncludesExcludes,
    SupervisionManagementOperationsStaffIncludesExcludes,
    SupervisionProgrammaticStaffIncludesExcludes,
    SupervisionStaffDimIncludesExcludes,
    SupervisionStaffIncludesExcludes,
    SupervisionVacantStaffIncludesExcludes,
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
    system=System.SUPERVISION,
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
    system=System.SUPERVISION,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) allocated to your agency's supervision functions.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.SUPERVISION_IN_ANOTHER_AGENCY_BUDGET,
            value_type=ValueType.MULTIPLE_CHOICE,
            label="Is community supervision included in another agency's budget?",
            required=True,
            multiple_choice_options=YesNoContext,
        ),
        Context(
            key=ContextKey.SUPERVISION_STRUCTURE,
            value_type=ValueType.TEXT,
            label="Please specify supervision structure type (unified, etc.).",
            required=True,
        ),
    ],
)

total_staff = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the agency for the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
    definitions=[
        Definition(
            term="Full-time staff",
            definition="Number of people employed in a full-time (0.9+) capacity.",
        )
    ],
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=SupervisionStaffType,
            required=False,
            dimension_to_includes_excludes={
                SupervisionStaffType.SUPERVISION: IncludesExcludesSet(
                    members=SupervisionStaffDimIncludesExcludes,
                    excluded_set={SupervisionStaffDimIncludesExcludes.VACANT},
                ),
                SupervisionStaffType.MANAGEMENT_AND_OPERATIONS: IncludesExcludesSet(
                    members=SupervisionManagementOperationsStaffIncludesExcludes,
                    excluded_set={
                        SupervisionManagementOperationsStaffIncludesExcludes.VACANT
                    },
                ),
                SupervisionStaffType.CLINICAL_OR_MEDICAL: IncludesExcludesSet(
                    members=SupervisionClinicalMedicalStaffIncludesExcludes,
                    excluded_set={
                        SupervisionClinicalMedicalStaffIncludesExcludes.VACANT,
                    },
                ),
                SupervisionStaffType.PROGRAMMATIC: IncludesExcludesSet(
                    members=SupervisionProgrammaticStaffIncludesExcludes,
                    excluded_set={
                        SupervisionProgrammaticStaffIncludesExcludes.VOLUNTEER,
                        SupervisionProgrammaticStaffIncludesExcludes.VACANT,
                    },
                ),
                SupervisionStaffType.VACANT: IncludesExcludesSet(
                    members=SupervisionVacantStaffIncludesExcludes,
                    excluded_set={
                        SupervisionVacantStaffIncludesExcludes.FILLED,
                    },
                ),
            },
            dimension_to_description={
                SupervisionStaffType.SUPERVISION: "The number of full-time equivalent positions that work directly with people who are on supervision and are responsible for their supervision and case management.",
                SupervisionStaffType.MANAGEMENT_AND_OPERATIONS: "The number of full-time equivalent positions that do not work directly with people who are supervised in the community but support the day-to-day operations of the supervision agency.",
                SupervisionStaffType.CLINICAL_OR_MEDICAL: "The number of full-time equivalent positions that work directly with people on probation, parole, or other community supervision and are responsible for their physical or mental health.",
                SupervisionStaffType.PROGRAMMATIC: "The number of full-time equivalent positions that provide services and programming to people on community supervision but are not medical or clinical staff.",
                SupervisionStaffType.OTHER: "The number of full-time equivalent positions dedicated to the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency that are not supervision staff, management and operations staff, clinical or medical staff, or programmatic staff.",
                SupervisionStaffType.UNKNOWN: "The number of full-time equivalent positions dedicated to the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency that are of an unknown type.",
                SupervisionStaffType.VACANT: "The number of full-time equivalent positions of any type dedicated to the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency that are budgeted but not currently filled.",
            },
        ),
    ],
    includes_excludes=IncludesExcludesSet(
        members=SupervisionStaffIncludesExcludes,
        excluded_set={
            SupervisionStaffIncludesExcludes.VOLUNTEER,
            SupervisionStaffIncludesExcludes.INTERN,
        },
    ),
)

supervision_violations = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.SUPERVISION_VIOLATIONS,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Supervision Violations",
    description="Measures the number of individuals with at least one violation during the reporting period.",
    reporting_note="Report the most serious violation type incurred during the reporting period.",
    definitions=[
        Definition(
            term="Violation",
            definition="An event in which an individual under supervision ignores, errs, or otherwise breaks a condition of their supervision as defined by your agency. Violations may involve the commission of a new offense or failing to meet agreed upon parameters (appearance in court, a positive drug test, attendance in programming). Record violations whether or not a resulting action may be revocation.",
        )
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SupervisionViolationType, required=True)
    ],
)

new_supervision_cases = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.SUPERVISION_STARTS,
    category=MetricCategory.POPULATIONS,
    display_name="New Supervision Cases",
    description="Measures the number of new cases referred to your agency for supervision.",
    definitions=[
        Definition(
            term="Active supervision",
            definition="A case in which the individual under supervision is required to  regularly report to a supervision officer or court in person or phone.",
        )
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_note="Record only individuals entering for a new supervision term, not for an extension or reinstatement of a prior case.",
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SupervisionCaseType, required=False)
    ],
)

individuals_under_supervision = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Individuals under Supervision",
    description="Measures the number individuals currently under the supervision of your agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=SupervisionIndividualType,
            required=False,
            display_name="Supervision Type",
        ),
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(dimension=GenderRestricted, required=True),
    ],
)

supervision_terminations = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.SUPERVISION_TERMINATIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Supervision Terminations",
    description="Measures the number of individuals exiting from supervision.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SupervisionTerminationType, required=True)
    ],
)

reconviction_while_on_supervision = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.RECONVICTIONS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Reconviction while on Supervision",
    description="Measures the number of individuals convicted of a new offense while under supervision in the previous calendar year.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(dimension=NewOffenseType, required=False)
    ],
)
