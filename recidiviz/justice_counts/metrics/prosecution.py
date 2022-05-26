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

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import (
    CaseSeverityType,
    DispositionType,
    ProsecutionAndDefenseStaffType,
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

annual_budget = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of the office.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.PRIMARY_FUNDING_SOURCE,
            value_type=ValueType.TEXT,
            label="Please describe the primary funding source.",
            required=False,
        ),
    ],
)

total_staff = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total Staff",
    description="Measures the number of full-time staff employed by the office.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(dimension=ProsecutionAndDefenseStaffType, required=False)
    ],
)

cases_rejected = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DECLINED,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Cases Rejected",
    description="Measures the number of cases referred to the prosecutor that were rejected for prosecution, by case severity.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[
        Context(
            key=ContextKey.ANOTHER_AGENCY_CAN_FILE_CHARGES,
            value_type=ValueType.BOOLEAN,
            label="Does another agency in the jurisdiction have the authority to file charges/cases directly with the court?",
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CaseSeverityType, required=False)
    ],
)

cases_referred = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_REFERRED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Referred (Intake)",
    description="Measures the number of cases referred to the office for prosecution (intake), by case severity.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CaseSeverityType, required=False)
    ],
)

caseloads = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASELOADS,
    category=MetricCategory.POPULATIONS,
    display_name="Caseloads",
    description="Measures the average caseload per attorney in the office.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[
        Context(
            key=ContextKey.METHOD_OF_CALCULATING_CASELOAD,
            value_type=ValueType.TEXT,
            label="What is the office's method of calculating caseload?",
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CaseSeverityType, required=False)
    ],
)

cases_disposed = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DISPOSED,
    category=MetricCategory.POPULATIONS,
    display_name="Caseloads",
    description="Measures the number of cases disposed by the office.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=DispositionType, required=False)
    ],
)

cases_rejected_by_demographic = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DECLINED,
    category=MetricCategory.EQUITY,
    display_name="Cases rejected, by demographic",
    description="Measures the percent of cases that were rejected for prosecution, by demographic",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=GenderRestricted, required=False),
        AggregatedDimension(dimension=RaceAndEthnicity, required=False),
    ],
)

violations = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.VIOLATIONS_WITH_DISCIPLINARY_ACTION,
    category=MetricCategory.FAIRNESS,
    display_name="Violations filed against attorneys in the office resulting in disciplinary action",
    description="Measures the percent of violations filed against attorneys in the office that result in disciplinary actions.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
)
