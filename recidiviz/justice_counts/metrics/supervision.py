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
"""Defines all Justice Counts metrics for the Supervision."""

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
    MetricCategory,
    MetricDefinition,
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
    description="Measures the number of residents in the agency's jurisdiction.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY, ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(dimension=GenderRestricted, required=True),
    ],
)

annual_budget = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of the agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.PRIMARY_FUNDING_SOURCE,
            value_type=ValueType.TEXT,
            label="Is community supervision included in another agency’s budget? Please specify supervision structure (unified, etc.).",
            required=True,
        )
    ],
)

total_staff = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total Staff",
    description="Measures the number of full-time staff employed by the agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SupervisionStaffType, required=False)
    ],
)

supervision_violations = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.SUPERVISION_VIOLATIONS,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Supervision Violations",
    description="Measures the number of individuals with at least one violation during the reporting period, by violation type.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SupervisionViolationType, required=False)
    ],
)

new_supervision_cases = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="New Supervision Cases",
    description="Measures the number of individuals referred to the agency for supervision.",
    measurement_type=MeasurementType.DELTA,
    reporting_note="Record only individuals entering for a new supervision term, not for an extension or reinstatement of a prior case.",
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SupervisionCaseType, required=False)
    ],
)

individuals_under_supervision_by_type = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Individuals under Supervision",
    description="Measures the number individuals currently under the supervision of the agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SupervisionIndividualType, required=False),
    ],
)

supervision_terminations = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.SUPERVISION_TERMINATIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Supervision Terminations",
    description="Measures the number of individuals exiting from supervision, by termination type.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SupervisionTerminationType, required=False)
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

individuals_under_supervision_by_demographic = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.EQUITY,
    display_name="Individuals under Supervision",
    description="Measures the number of individuals under supervision",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Together, these numbers should total the Population metric.",
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=False),
        AggregatedDimension(dimension=GenderRestricted, required=False),
    ],
)
