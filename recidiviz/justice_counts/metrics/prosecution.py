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
    system=System.PROSECUTION,
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
    system=System.PROSECUTION,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Annual Budget",
    description="Measures the total annual budget (in dollars) of your office.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Agencies should report their budget for the criminal services they provide, if it is possible to delineate.",
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
    description="Measures the number of full-time staff employed by your office.",
    definitions=[
        Definition(
            term="Full time staff",
            definition="Number of people employed in a full-time (0.9+) capacity.",
        )
    ],
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="If multiple staff are part-time but make up a full-time position of employment, this may count as one full time staff position filled.",
    aggregated_dimensions=[
        AggregatedDimension(dimension=ProsecutionAndDefenseStaffType, required=False)
    ],
)

cases_rejected = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DECLINED,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Cases Rejected",
    description="Measures the number of cases referred to the prosecutor that were rejected for prosecution.",
    definitions=[
        Definition(
            term="Rejected",
            definition="A case for which a prosecutor has declined to bring referred charges against an individual. Rejected cases are those in which the prosecutor has refused to bring/file any  of the referred charges.",
        )
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[
        Context(
            key=ContextKey.ANOTHER_AGENCY_CAN_FILE_CHARGES,
            value_type=ValueType.MULTIPLE_CHOICE,
            label="Does another agency in the jurisdiction have the authority to file charges/cases directly with the court?",
            required=True,
            multiple_choice_options=YesNoContext,
        ),
        Context(
            key=ContextKey.ADDITIONAL_PROSECUTION_OUTCOMES,
            value_type=ValueType.TEXT,
            label="Please describe any additional outcomes of case review available to attorneys in your agency, other than rejected or filed.",
            required=False,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=CaseSeverityType, required=False),
        AggregatedDimension(dimension=GenderRestricted, required=False),
        AggregatedDimension(dimension=RaceAndEthnicity, required=False),
    ],
)

cases_referred = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_REFERRED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Referred (Intake)",
    definitions=[
        Definition(
            term="Referral",
            definition="A case involving one or more charges brought to the prosecutor for review before being filed against an individual. This may include cases brought by law enforcement or other means for prosecutorial review.",
        )
    ],
    description="Measures the number of cases referred to your office for prosecution (intake).",
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
    description="Measures the average caseload per attorney in your office.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="These elements may not be necessary if your office already calculates their average caseloads by type on a monthly basis. Accept your office's calculation with required context.",
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
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DISPOSED,
    category=MetricCategory.POPULATIONS,
    display_name="Cases Disposed",
    definitions=[
        Definition(
            term="Disposition",
            definition="The initial decision made in the adjudication of the criminal case. Report the disposition for the case as a whole, such that if two charges are dismissed and one is plead, the case disposition is a conviction by plea.",
        )
    ],
    description="Measures the number of cases disposed by your office.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="To the extent possible, report the initial disposition on the case and not any post-conviction decisions. If your case management system requires that post-conviction decisions overwrite the initial disposition, note this as additional context.",
    aggregated_dimensions=[
        AggregatedDimension(dimension=DispositionType, required=False)
    ],
)

violations = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.VIOLATIONS_WITH_DISCIPLINARY_ACTION,
    category=MetricCategory.FAIRNESS,
    display_name="Violations",
    definitions=[
        Definition(
            term="Violation",
            definition="A complaint filed against an attorney pertaining to an error in judgment or other prosecutorial misconduct.",
        )
    ],
    description="Measures the percent of violations filed against attorneys in your office that result in disciplinary actions.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
)
