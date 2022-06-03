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
    Definition,
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
    description="Measures the annual budget (in dollars) of the criminal courts.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
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
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Total Staff",
    definitions=[
        Definition(
            term="Full time staff",
            definition="Number of people employed in a full-time (0.9+) capacity.",
        )
    ],
    reporting_note="If multiple staff are part-time but make up a full-time position of employment, this may count as one full time staff position filled.",
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
    definitions=[
        Definition(
            term="Pretrial release",
            definition="The initial decision that an individual does not need to remain in custody while awaiting trial. This may involve nonmonetary release or monetary bail, or release with conditions (including electronic monitoring).",
        )
    ],
    display_name="Pretrial Releases",
    description="Measures the number of cases in which an individual is released while awaiting trial.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="As much as possible, report the initial decision made by the court or on a bail schedule. Many jurisdictions may overwrite this decision if bail is modified at any point. This should be noted.",
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
    reporting_note="Report cases under the most restrictive sentence (from death to financial obligations only).",
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_METHOD_FOR_TIME_SERVED,
            value_type=ValueType.TEXT,
            label="Please describe your jurisdiction’s method for recording time served.",
            required=True,
        )
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=SentenceType, required=False),
        AggregatedDimension(dimension=RaceAndEthnicity, required=False),
        AggregatedDimension(dimension=GenderRestricted, required=False),
    ],
)

criminal_case_filings = MetricDefinition(
    system=System.COURTS_AND_PRETRIAL,
    metric_type=MetricType.CASES_FILED,
    category=MetricCategory.POPULATIONS,
    display_name="Criminal Case Filings",
    description="Measures the number of new criminal cases filed with the court.",
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
    definitions=[
        Definition(
            term="Disposition",
            definition="The initial decision made in the adjudication of the criminal case. Report the disposition for the case as a whole, such that if two charges are dismissed and one is plead, the case disposition is a conviction by plea.",
        )
    ],
    description="Measures the number of cases disposed of in the court.",
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
    reporting_note="Choose all cases in previous year and identify if they were released, and whether they had any new offense between release and reporting date.",
    aggregated_dimensions=[
        AggregatedDimension(dimension=CourtCaseType, required=False)
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
