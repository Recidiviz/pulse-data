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
    FundingType,
    StaffType,
)
from recidiviz.justice_counts.includes_excludes.prosecution import (
    ProsecutionAdministrativeStaffIncludesExcludes,
    ProsecutionAdvocateStaffIncludesExcludes,
    ProsecutionCaseloadIncludesExcludes,
    ProsecutionFelonyCaseloadIncludesExcludes,
    ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes,
    ProsecutionFundingGrantsIncludesExcludes,
    ProsecutionFundingIncludesExcludes,
    ProsecutionFundingStateAppropriationsIncludesExcludes,
    ProsecutionInvestigativeStaffIncludesExcludes,
    ProsecutionLegalStaffIncludesExcludes,
    ProsecutionMisdemeanorCaseloadIncludesExcludes,
    ProsecutionMixedCaseloadIncludesExcludes,
    ProsecutionStaffIncludesExcludes,
    ProsecutionVacantStaffIncludesExcludes,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Context,
    Definition,
    IncludesExcludesSet,
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
    disabled=True,
)

funding = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of the prosecution office to process criminal cases.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    # TODO(#17577)
    includes_excludes=IncludesExcludesSet(
        members=ProsecutionFundingIncludesExcludes,
        excluded_set={
            ProsecutionFundingIncludesExcludes.NON_CRIMINAL_CASE_PROCESSING,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=False,
            dimension_to_description={
                FundingType.STATE_APPROPRIATIONS: "The amount of funding appropriated by the state for the operation and maintenance of the prosecutor’s office to process criminal cases.",
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATIONS: "The amount of funding appropriated by counties or municipalities for the operation and maintenance of the prosecutor’s office to process criminal cases.",
                FundingType.GRANTS: "The amount of funding derived by the office through grants and awards to be used for the operation and maintenance of the prosecutor’s office to process criminal cases.",
                FundingType.OTHER: "The amount of funding to be used for the operation and maintenance of the prosecutor’s office to process criminal cases that is not appropriations from the state, appropriations from counties or cities, or funding from grants.",
                FundingType.UNKNOWN: "The amount of funding for the operation and maintenance of the prosecutor’s office to process criminal cases for which the source is not known.",
            },
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATIONS: IncludesExcludesSet(
                    members=ProsecutionFundingStateAppropriationsIncludesExcludes,
                    excluded_set={
                        ProsecutionFundingStateAppropriationsIncludesExcludes.PROPOSED,
                        ProsecutionFundingStateAppropriationsIncludesExcludes.PRELIMINARY,
                        ProsecutionFundingStateAppropriationsIncludesExcludes.GRANTS,
                    },
                ),
                FundingType.COUNTY_OR_MUNICIPAL_APPROPRIATIONS: IncludesExcludesSet(
                    members=ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes,
                    excluded_set={
                        ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes.PROPOSED,
                        ProsecutionFundingCountyOrMunicipalAppropriationsIncludesExcludes.PRELIMINARY,
                    },
                ),
                FundingType.GRANTS: IncludesExcludesSet(
                    members=ProsecutionFundingGrantsIncludesExcludes,
                ),
            },
        ),
    ],
)

staff = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the office to process criminal cases.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        ProsecutionStaffIncludesExcludes,
        excluded_set={
            ProsecutionStaffIncludesExcludes.VOLUNTEER,
            ProsecutionStaffIncludesExcludes.INTERN,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_description={
                StaffType.LEGAL_STAFF: "The number of full-time equivalent positions that are responsible for their own criminal caseload or for performing tasks that have a legal function in support of that caseload.",
                StaffType.ADVOCATE_STAFF: "The number of full-time equivalent positions that advise, counsel, or assist victims or witnesses of crime.",
                StaffType.ADMINISTRATIVE: "The number of full-time equivalent positions that support legal and clerical policies and logistics to process criminal cases.",
                StaffType.INVESTIGATIVE_STAFF: "The number of full-time equivalent positions that are responsible for gathering evidence to support criminal prosecutorial cases, inquiries into the details of a criminal case, and gathering evidence.",
                StaffType.OTHER: "The number of full-time equivalent positions to process criminal cases that are not legal staff, victim-witness advocate staff, administrative staff, investigative staff, or staff with unknown position types but are another type of staff position.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions to process criminal cases that are of an unknown type.",
                StaffType.VACANT_POSITIONS: "The number of full-time equivalent positions to process criminal cases of any type that are budgeted but not currently filled.",
            },
            dimension_to_includes_excludes={
                StaffType.LEGAL_STAFF: IncludesExcludesSet(
                    members=ProsecutionLegalStaffIncludesExcludes
                ),
                StaffType.ADVOCATE_STAFF: IncludesExcludesSet(
                    members=ProsecutionAdvocateStaffIncludesExcludes
                ),
                StaffType.ADMINISTRATIVE: IncludesExcludesSet(
                    members=ProsecutionAdministrativeStaffIncludesExcludes,
                    excluded_set={
                        ProsecutionAdministrativeStaffIncludesExcludes.INVESTIGATIVE_STAFF
                    },
                ),
                StaffType.INVESTIGATIVE_STAFF: IncludesExcludesSet(
                    members=ProsecutionInvestigativeStaffIncludesExcludes,
                ),
                StaffType.VACANT_POSITIONS: IncludesExcludesSet(
                    members=ProsecutionVacantStaffIncludesExcludes,
                    excluded_set={ProsecutionVacantStaffIncludesExcludes.FILLED},
                ),
            },
        )
    ],
)

cases_declined = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASES_DECLINED,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Cases Declined",
    description="Measures the number of cases referred to the prosecutor that were declined for prosecution.",
    definitions=[
        Definition(
            term="Declined",
            definition="A case for which a prosecutor has declined to bring referred charges against an individual. Declined cases are those in which the prosecutor has refused to bring/file any of the referred charges.",
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
            label="Please describe any additional outcomes of case review available to attorneys in your agency, other than declined or filed.",
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

caseload = MetricDefinition(
    system=System.PROSECUTION,
    metric_type=MetricType.CASELOADS,
    category=MetricCategory.POPULATIONS,
    display_name="Caseload",
    description="The ratio of the number of people with open criminal cases to the number of staff carrying a criminal caseload.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#17577)
    includes_excludes=IncludesExcludesSet(
        members=ProsecutionCaseloadIncludesExcludes,
        excluded_set={ProsecutionCaseloadIncludesExcludes.UNASSIGNED_CASES},
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=CaseSeverityType,
            required=False,
            dimension_to_description={
                CaseSeverityType.FELONY: "The ratio of the number of people with open felony cases to the number of staff carrying a felony caseload.",
                CaseSeverityType.MISDEMEANOR: "The ratio of the number of people with open misdemeanor cases to the number of staff carrying a misdemeanor caseload.",
                CaseSeverityType.MIXED: "The ratio of the number of people with open felony and misdemeanor cases to the number of staff carrying a mixed (felony and misdemeanor) caseload.",
                CaseSeverityType.OTHER: "The ratio of the number of people with open criminal cases that are not felony or misdemeanor cases to the number of staff carrying a criminal caseload that does not comprise felony or misdemeanor cases.",
                CaseSeverityType.UNKNOWN: "The ratio of the number of people with open criminal cases of unknown severity to the number of staff carrying a criminal caseload of unknown severity.",
            },
            dimension_to_includes_excludes={
                CaseSeverityType.FELONY: IncludesExcludesSet(  # TODO(#18071)
                    members=ProsecutionFelonyCaseloadIncludesExcludes,
                    excluded_set={
                        ProsecutionFelonyCaseloadIncludesExcludes.UNASSIGNED_CASES
                    },
                ),
                CaseSeverityType.MISDEMEANOR: IncludesExcludesSet(  # TODO(#18071)
                    members=ProsecutionMisdemeanorCaseloadIncludesExcludes,
                    excluded_set={
                        ProsecutionMisdemeanorCaseloadIncludesExcludes.UNASSIGNED_CASES
                    },
                ),
                CaseSeverityType.MIXED: IncludesExcludesSet(  # TODO(#18071)
                    members=ProsecutionMixedCaseloadIncludesExcludes,
                    excluded_set={
                        ProsecutionMixedCaseloadIncludesExcludes.UNASSIGNED_FELONY_CASES,
                        ProsecutionMixedCaseloadIncludesExcludes.UNASSIGNED_MISDEMEANOR_CASES,
                    },
                ),
            },
        )
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
    description="Measures the percent of violations filed against attorneys in your office that have resulted in disciplinary actions.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
)
