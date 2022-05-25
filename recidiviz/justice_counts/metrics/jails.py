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
    JailForceType,
    JailPopulationType,
    JailStaffType,
    ReadmissionType,
    ReleaseType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
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
    system=System.JAILS,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of the jail system.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Sheriff offices should report on the detention function.",
    specified_contexts=[
        Context(
            key=ContextKey.PRETRIAL_SUPERVISION_FUNCTION,
            value_type=ValueType.BOOLEAN,
            label="Does the annual budget include the pretrial supervision function?",
            required=True,
        ),
        Context(
            key=ContextKey.PRIMARY_FUNDING_SOURCE,
            value_type=ValueType.TEXT,
            label="Please describe the primary funding source.",
            required=False,
        ),
    ],
)
total_staff = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total Staff",
    description="Measures the number of full-time staff employed by the jail system.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.INCLUDES_PROGRAMATIC_STAFF,
            value_type=ValueType.BOOLEAN,
            label="Does the staff count includes programatic staff?",
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=JailStaffType, required=False)
    ],
)

readmissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.READMISSIONS,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Readmissions",
    description="Measures the number of individuals admitted who had at least one other jail admission within the prior year.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_ADMISSION,
            value_type=ValueType.TEXT,
            label="Please provide the agency's definition of admission",
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=ReadmissionType, required=False)
    ],
)

admissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Admissions",
    description="Measures the number of new admissions to the jail system.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_ADMISSION,
            value_type=ValueType.TEXT,
            label="Please provide the agency's definition of admission",
            required=True,
        ),
        Context(
            key=ContextKey.INCLUDES_VIOLATED_CONDITIONS,
            value_type=ValueType.BOOLEAN,
            label="Are the individuals admitted for violation of conditions counted within the total population?",
            required=False,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=JailPopulationType, required=False)
    ],
)

average_daily_population = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Average Daily Population",
    description="Measures the average daily population held in the jail system.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Calculate the average against a 30-day month",
    specified_contexts=[
        Context(
            key=ContextKey.INCLUDES_VIOLATED_CONDITIONS,
            value_type=ValueType.BOOLEAN,
            label="Individuals admitted for violation of conditions are counted within the total population.",
            required=False,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=JailPopulationType, required=False)
    ],
)
releases = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Releases",
    description="Measures the number of releases from the jail system",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[AggregatedDimension(dimension=ReleaseType, required=False)],
)

staff_use_of_force_incidents = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Staff Use of Force Incidents",
    description="Measures the number of staff use of force incidents",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_USE_OF_FORCE,
            value_type=ValueType.TEXT,
            label="Please provide the agency's definition of 'use of force'.",
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=JailForceType, required=False)
    ],
)

average_daily_population_by_race_and_ethnicity = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.EQUITY,
    display_name="Average Daily Population, by race/ethnicity",
    description="Measures the average daily jail population of each race/ethnic group.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="This is the average daily population for each group. Calculate the average against a 30-day month",
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=True)
    ],
)
average_daily_population_by_gender = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.EQUITY,
    display_name="Average Daily Population, by gender",
    description="Measures the average daily jail population of gender.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="This is the average daily population for each group. Calculate the average against a 30-day month",
    aggregated_dimensions=[
        AggregatedDimension(dimension=GenderRestricted, required=True)
    ],
)

grievances_upheld = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.GRIEVANCES_UPHELD,
    category=MetricCategory.FAIRNESS,
    display_name="Grievances upheld",
    description="Measures the number of grievances filed with the institution that were upheld.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
)
