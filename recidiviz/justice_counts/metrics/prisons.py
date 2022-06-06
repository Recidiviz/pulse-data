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
    CorrectionalFacilityForceType,
    CorrectionalFacilityStaffType,
    PrisonPopulationType,
    PrisonReleaseTypes,
    ReadmissionType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
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

residents = MetricDefinition(
    system=System.PRISONS,
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
    system=System.PRISONS,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of the state correctional institutions.",
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
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total Staff",
    description="Measures the number of full time staff employed by the agency.",
    definitions=[
        Definition(
            term="Full time staff",
            definition="Number of people employed in a full-time (0.9+) capacity.",
        )
    ],
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
        AggregatedDimension(dimension=CorrectionalFacilityStaffType, required=False)
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
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_ADMISSION,
            value_type=ValueType.TEXT,
            label="Please provide your agency's definition of admission.",
            required=True,
        )
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=ReadmissionType, required=False)
    ],
)

admissions = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Admissions",
    description="Measures the number of new admissions to the state correctional system.",
    measurement_type=MeasurementType.DELTA,
    reporting_note="Report individuals in the most serious category (new sentence > violation > hold).",
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
            label="Does your count include individuals admitted for violation of conditions?",
            required=False,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=PrisonPopulationType, required=False)
    ],
)

average_daily_population = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Average Daily Population",
    description="Measures the average daily population of individuals held in the state corrections system.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Calculate the average against a 30-day month. Report individuals in the most serious category (new sentence > violation > hold).",
    aggregated_dimensions=[
        AggregatedDimension(dimension=PrisonPopulationType, required=False),
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(dimension=GenderRestricted, required=True),
    ],
)

releases = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Releases",
    description="Measures the number of releases from the state corrections system.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Exclude temporary release (work release, appointment, court hearing, etc.).",
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_SUPERVISION,
            value_type=ValueType.TEXT,
            label="Please provide the agency's definition of supervision (i.e. probation, parole, or both).",
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=PrisonReleaseTypes, required=False)
    ],
)

staff_use_of_force_incidents = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Staff Use of Force Incidents",
    description="Measures the number of staff use of force incidents.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Select the most serious type of force used per incident.",
    definitions=[
        Definition(
            term="Use of force incident",
            definition="An event in which an officer uses force towards or in the vicinity of an individual incarcerated. The AJA focuses on uses of force resulting in injury or a discharge of a weapon.  Count all uses of force occurring during the same event as one incident.",
        )
    ],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_USE_OF_FORCE,
            value_type=ValueType.TEXT,
            label='Please provide the agency\'s definition of "use of force".',
            required=True,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CorrectionalFacilityForceType, required=False)
    ],
)

grievances_upheld = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.GRIEVANCES_UPHELD,
    category=MetricCategory.FAIRNESS,
    display_name="Grievances upheld",
    description="Measures the number of grievances filed with the institution that were upheld.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    definitions=[
        Definition(
            term="Grievance",
            definition="A complaint or question filed with the institution by an individual incarcerated regarding their experience, with procedures, treatment, or interaction with officers.",
        )
    ],
)
