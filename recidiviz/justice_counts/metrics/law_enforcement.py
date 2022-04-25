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
"""Defines all Justice Counts metrics for the Law Enforcement system."""

from recidiviz.justice_counts.dimensions.corrections import PopulationType
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    ForceType,
    OffenseType,
    SheriffBudgetType,
)
from recidiviz.justice_counts.dimensions.person import (
    Gender,
    RaceAndEthnicity,
    StaffType,
)
from recidiviz.justice_counts.metrics.constants import ContextKey, ContextType
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    Definition,
    FilteredDimension,
    MetricCategory,
    MetricDefinition,
    ReportingFrequency,
)
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    System,
)

annual_budget = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of the agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Sheriff offices report on budget for patrol and detention separately",
    specified_contexts=[
        Context(
            key=ContextKey.PRIMARY_FUNDING_SOURCE,
            context_type=ContextType.TEXT,
            label="Primary funding source.",
            required=False,
        )
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=SheriffBudgetType, required=False, should_sum_to_total=True
        )
    ],
)

residents = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Jurisdiction residents",
    description="Measures the number of residents in the agency's jurisdiction.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY, ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_AREA,
            context_type=ContextType.NUMBER,
            label="The land size (area) of the jurisdiction",
            required=False,
        )
    ],
    filtered_dimensions=[FilteredDimension(dimension=PopulationType.RESIDENTS)],
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=True)
    ],
)


calls_for_service = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.CALLS_FOR_SERVICE,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Calls for Service",
    description="Measures the number of calls for service routed to the agency, by call type.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Do not include calls that are officer-initiated.",
    definitions=[
        Definition(
            term="Calls for service",
            definition="""One case that represents a request for police service generated
            by the community and received through an emergency or non-emergency method 
            (911, 311, 988, online report). Count all calls for service, regardless of
            whether an underlying incident report was filed.""",
        )
    ],
    specified_contexts=[
        Context(
            key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED,
            context_type=ContextType.BOOLEAN,
            label="Whether number includes all calls or just calls responded to.",
            required=True,
        ),
        Context(
            key=ContextKey.AGENCIES_AVAILABLE_FOR_RESPONSE,
            context_type=ContextType.TEXT,
            label="All agencies available for response.",
            required=False,
        ),
    ],
    aggregated_dimensions=[AggregatedDimension(dimension=CallType, required=True)],
)

police_officers = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total police officers in the agency's jurisdictions",
    description="Measures the number of police officers in the agency's jurisdiction.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    filtered_dimensions=[FilteredDimension(dimension=StaffType.POLICE_OFFICERS)],
)

reported_crime = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.REPORTED_CRIME,
    category=MetricCategory.POPULATIONS,
    display_name="The number of crimes reported to the agency,",
    description="Measures the number of crimes reported to the agency, by offense type.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[AggregatedDimension(dimension=OffenseType, required=True)],
)

total_arrests = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.ARRESTS,
    category=MetricCategory.POPULATIONS,
    display_name="Total arrests made by the agency.",
    description="Measures the number of arrests made by the agency, by offense type.",
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_ARREST,
            context_type=ContextType.TEXT,
            label="The jurisdiction definition of arrest",
            required=True,
        )
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[AggregatedDimension(dimension=OffenseType, required=True)],
)

arrests_by_race_and_ethnicity = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.ARRESTS,
    category=MetricCategory.EQUITY,
    display_name="Total arrests made by the agency, by race and ethnicity.",
    description="Measures the number of arrests for each race/ethnic group.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=True)
    ],
)

arrests_by_gender = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.ARRESTS,
    category=MetricCategory.EQUITY,
    display_name="Total arrests made by the agency, by gender.",
    description="Measures the number of arrests for each gender.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[AggregatedDimension(dimension=Gender, required=True)],
)

officer_use_of_force_incidents = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Total use of force incidents documented by the agency.",
    description="Measures the number use of force incidents.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_USE_OF_FORCE,
            context_type=ContextType.TEXT,
            label="The jurisdiction definition of use of force",
            required=True,
        )
    ],
    aggregated_dimensions=[AggregatedDimension(dimension=ForceType, required=True)],
)

civilian_complaints_sustained = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.COMPLAINTS_SUSTAINED,
    category=MetricCategory.FAIRNESS,
    reporting_note="Disclaimer: Many factors can lead to a complaint being filed, and to a final decision on the complaint. These factors may also vary by agency and jurisdiction.",
    display_name="Total number of complaints filed against officers in the agency that were ultimately sustained.",
    description="Measures the number of complaints filed against officers in the agency that were ultimately sustained.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
)
