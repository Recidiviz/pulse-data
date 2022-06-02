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
"""Defines all Justice Counts metrics for Courts and Pretrial."""

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.courts import (
    CourtCaseType,
    CourtReleaseType,
    CourtsCaseSeverityType,
    CourtStaffType,
    SentenceType,
)
from recidiviz.justice_counts.dimensions.person import (
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.prosecution import DispositionType
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    MetricCategory,
    MetricDefinition,
    ReportingFrequency,
)
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    System,
)

residents = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
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
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of the criminal courts.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.PRIMARY_FUNDING_SOURCE,
            value_type=ValueType.TEXT,
            label="Please descrbe your primary budget source.",
            required=False,
        )
    ],
)

total_staff = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total Staff",
    description="Measures the number of full-time staff employed by the criminal courts.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CourtStaffType, required=False)
    ],
)

pretrial_releases = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.PRETRIAL_RELEASES,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Pretrial Releases",
    description="Measures the number of cases in which an individual is released while awaiting trial.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CourtReleaseType, required=False)
    ],
)


sentences_imposed = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.SENTENCES,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Sentences Imposed",
    description="Measures the number of cases with a sentence imposed.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_METHOD_FOR_TIME_SERVED,
            value_type=ValueType.TEXT,
            label="Please describe your jurisdiction’s method for recording time served.",
            required=True,
        )
    ],
    aggregated_dimensions=[AggregatedDimension(dimension=SentenceType, required=False)],
)

criminal_case_filings = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.CASES_FILED,
    category=MetricCategory.POPULATIONS,
    display_name="Sentences Imposed",
    description="Measures the number of new criminal cases filed with the court, by case severity.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[
        Context(
            key=ContextKey.AGENCIES_AUTHORIZED_TO_FILE_CASES,
            value_type=ValueType.TEXT,
            label="Please provide the agencies authorized to file cases directly with the court.",
            required=True,
        )
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CourtsCaseSeverityType, required=False)
    ],
)

cases_disposed = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.CASES_DISPOSED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Disposed",
    description="Measures the number of cases disposed of in the court, by disposition type.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=DispositionType, required=False)
    ],
)

new_offenses_while_on_pretrial_release = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.ARRESTS_ON_PRETRIAL_RELEASE,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="New offenses while on Pretrial Release",
    description="Measures the number of cases in which an individual was released pending trial in the previous calendar year and was arrested for a new offense.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CourtCaseType, required=False)
    ],
)

sentences_imposed_by_demographic = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.SENTENCES,
    category=MetricCategory.EQUITY,
    display_name="Sentences Imposed, by demographic",
    description="Measures the number of cases by demographic.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=False),
        AggregatedDimension(dimension=GenderRestricted, required=False),
    ],
)

cases_overturned = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.CASES_OVERTURNED_ON_APPEAL,
    category=MetricCategory.FAIRNESS,
    display_name="Cases Overturned on Appeal",
    description="Measures the number of cases for which the decision was overturned as a result of an appeal.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Disclaimer: Many factors can lead to a case being overturned and they aren't always reflection of fairness (e.g., jury question, procedural issues, case law interpretations, misconduct).",
)
