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
    Definition,
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
    system=System.DEFENSE,
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
)

annual_budget = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of your office.",
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
    system=System.DEFENSE,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total Staff",
    definitions=[
        Definition(
            term="Full time staff",
            definition="Number of people employed in a full-time (0.9+) capacity.",
        )
    ],
    description="Measures the number of full-time staff employed by your office.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="If multiple staff are part-time but make up a full-time position of employment, this may count as one full time staff position filled.",
    aggregated_dimensions=[
        AggregatedDimension(dimension=ProsecutionAndDefenseStaffType, required=False)
    ],
)

cases_appointed_counsel = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.CASES_APPOINTED_COUNSEL,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Appointed Counsel",
    definitions=[
        Definition(
            term="Appointed",
            definition="The point at which a case is assigned to an attorney in the office, whether or not the case is ultimately reassigned internally or externally due to conflict.",
        )
    ],
    description="Measures the number of new cases appointed counsel from your office, by case severity.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CaseSeverityType, required=False)
    ],
)

caseloads = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.CASELOADS,
    category=MetricCategory.POPULATIONS,
    display_name="Caseloads",
    description="Measures the average caseload per attorney in your office.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="These elements may not be necessary if the office already calculates their average caseloads by type on a monthly basis. Accept the office's calculation with required context.",
    specified_contexts=[
        Context(
            key=ContextKey.METHOD_OF_CALCULATING_CASELOAD,
            value_type=ValueType.TEXT,
            label="Please describe your office's method of calculating caseload.",
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CaseSeverityType, required=False)
    ],
)

cases_disposed = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.CASES_DISPOSED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Disposed",
    description="Measures the number of cases disposed by your office.",
    definitions=[
        Definition(
            term="Disposition",
            definition="The initial decision made in the adjudication of the criminal case. Report the disposition for the case as a whole, such that if two charges are dismissed and one is plead, the case disposition is a conviction by plea.",
        )
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=DispositionType, required=False),
        AggregatedDimension(dimension=GenderRestricted, required=False),
        AggregatedDimension(dimension=RaceAndEthnicity, required=False),
    ],
)

complaints = MetricDefinition(
    system=System.DEFENSE,
    metric_type=MetricType.COMPLAINTS_SUSTAINED,
    category=MetricCategory.FAIRNESS,
    display_name="Client Complaints against Counsel Sustained",
    description="Measures the number of complaints filed against attorneys in your office that are ultimately sustained.",
    definitions=[
        Definition(
            term="Complaint",
            definition="One case that represents one or more acts committed by the same attorney, or group of attorneys in the same case. Record complaints filed with the office or the state.",
        ),
        Definition(
            term="Sustained",
            definition="Found to be supported by the evidence, and may or may not result in disciplinary action.",
        ),
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Count only complaints filed against counsel.",
)
