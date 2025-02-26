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
"""Defines all Justice Counts metrics for Probation."""

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
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    Definition,
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

annual_budget = MetricDefinition(
    system=System.PROBATION,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Probation Budget",
    description="Measures the total annual budget (in dollars) allocated to probation.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.SUPERVISION_IN_ANOTHER_AGENCY_BUDGET,
            value_type=ValueType.MULTIPLE_CHOICE,
            label="Is probation included in another agency's budget?",
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
    system=System.PROBATION,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total Probation Staff",
    description="Measures the number of full-time probation staff employed by your agency.",
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
            display_name="Probation Staff Type",
        )
    ],
)

supervision_violations = MetricDefinition(
    system=System.PROBATION,
    metric_type=MetricType.SUPERVISION_VIOLATIONS,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Probation Violations",
    description="Measures the number of individuals with at least one probation violation during the reporting period.",
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
        AggregatedDimension(
            dimension=SupervisionViolationType,
            required=True,
            display_name="Probation Violation Type",
        )
    ],
)

new_supervision_cases = MetricDefinition(
    system=System.PROBATION,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="New Probation Cases",
    description="Measures the number of new probation cases referred to your agency.",
    definitions=[
        Definition(
            term="Active supervision",
            definition="A case in which the individual under supervision is required to regularly report to a supervision officer or court in person or phone.",
        )
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_note="Record only individuals entering for a new probation term, not for an extension or reinstatement of a prior case.",
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=SupervisionCaseType,
            required=False,
            display_name="Probation Case Type",
        )
    ],
)

individuals_under_supervision = MetricDefinition(
    system=System.PROBATION,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Individuals under Probation",
    description="Measures the number individuals currently on probation.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=SupervisionIndividualType,
            required=False,
            display_name="Probation Type",
        ),
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(dimension=GenderRestricted, required=True),
    ],
)

supervision_terminations = MetricDefinition(
    system=System.PROBATION,
    metric_type=MetricType.SUPERVISION_TERMINATIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Probation Terminations",
    description="Measures the number of individuals exiting from probation.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=SupervisionTerminationType,
            required=True,
            display_name="Probation Termination Type",
        )
    ],
)

reconviction_while_on_supervision = MetricDefinition(
    system=System.PROBATION,
    metric_type=MetricType.RECONVICTIONS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Reconviction while on Probation",
    description="Measures the number of individuals convicted of a new offense while on probation in the previous calendar year.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(dimension=NewOffenseType, required=False)
    ],
)
