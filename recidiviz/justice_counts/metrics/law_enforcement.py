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

from recidiviz.common.constants.justice_counts import ContextKey, ValueType
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    ForceType,
    LawEnforcementExpenseType,
    LawEnforcementFundingType,
    LawEnforcementStaffType,
    OffenseType,
)
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.law_enforcement import (
    LawEnforcementAssetForfeitureIncludesExcludes,
    LawEnforcementCivilianStaffIncludesExcludes,
    LawEnforcementCountyOrMunicipalAppropriation,
    LawEnforcementExpensesIncludesExcludes,
    LawEnforcementFacilitiesIncludesExcludes,
    LawEnforcementFundingIncludesExcludes,
    LawEnforcementGrantsIncludesExcludes,
    LawEnforcementMentalHealthStaffIncludesExcludes,
    LawEnforcementPersonnelIncludesExcludes,
    LawEnforcementPoliceOfficersIncludesExcludes,
    LawEnforcementStaffIncludesExcludes,
    LawEnforcementStateAppropriationIncludesExcludes,
    LawEnforcementTrainingIncludesExcludes,
    LawEnforcementVacantStaffIncludesExcludes,
    LawEnforcementVictimAdvocateStaffIncludesExcludes,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    CallsRespondedOptions,
    Context,
    Definition,
    IncludesExcludesSet,
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
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.RESIDENTS,
    category=MetricCategory.POPULATIONS,
    display_name="Jurisdiction Residents",
    description="Measures the number of residents in your agency's jurisdiction.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY, ReportingFrequency.ANNUAL],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_AREA,
            value_type=ValueType.NUMBER,
            label="Please provide the land size (area) of the jurisdiction in square miles.",
            required=False,
        )
    ],
    aggregated_dimensions=[
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(dimension=GenderRestricted, required=True),
    ],
    disabled=True,
)

funding = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for agency law enforcement activities.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        members=LawEnforcementFundingIncludesExcludes,
        excluded_set={
            LawEnforcementFundingIncludesExcludes.BIENNIUM_FUNDING,
            LawEnforcementFundingIncludesExcludes.MULTI_YEAR_APPROPRIATIONS,
            LawEnforcementFundingIncludesExcludes.JAIL_OPERATIONS,
            LawEnforcementFundingIncludesExcludes.JUVENILE_JAIL_OPERATIONS,
            LawEnforcementFundingIncludesExcludes.SUPERVISION_SERVICES,
        },
    ),
    specified_contexts=[],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=LawEnforcementFundingType,
            dimension_to_includes_excludes={
                LawEnforcementFundingType.STATE_APPROPRIATION: IncludesExcludesSet(
                    members=LawEnforcementStateAppropriationIncludesExcludes,
                    excluded_set={
                        LawEnforcementStateAppropriationIncludesExcludes.PRELIMINARY,
                        LawEnforcementStateAppropriationIncludesExcludes.PROPOSED,
                    },
                ),
                LawEnforcementFundingType.COUNTY_APPROPRIATION: IncludesExcludesSet(
                    members=LawEnforcementStateAppropriationIncludesExcludes,
                    excluded_set={
                        LawEnforcementCountyOrMunicipalAppropriation.PRELIMINARY,
                        LawEnforcementCountyOrMunicipalAppropriation.PROPOSED,
                    },
                ),
                LawEnforcementFundingType.ASSET_FORFEITURE: IncludesExcludesSet(
                    members=LawEnforcementAssetForfeitureIncludesExcludes,
                ),
                LawEnforcementFundingType.GRANTS: IncludesExcludesSet(
                    members=LawEnforcementGrantsIncludesExcludes,
                ),
            },
            dimension_to_description={
                LawEnforcementFundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for agency law enforcement activities.",
                LawEnforcementFundingType.COUNTY_APPROPRIATION: "The amount of funding appropriated by counties or municipalities for agency law enforcement activities.",
                LawEnforcementFundingType.ASSET_FORFEITURE: "The amount of funding derived by the agency through the seizure of assets.",
                LawEnforcementFundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for agency law enforcement activities.",
                LawEnforcementFundingType.OTHER: "The amount of funding to be used for agency law enforcement activities that is not appropriations from the state, appropriations from the county or city, asset forfeiture, or grants.",
                LawEnforcementFundingType.UNKNOWN: "The amount of funding to be used for agency law enforcement activities for which the source is not known.",
            },
            required=False,
        )
    ],
)

expenses = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.EXPENSES,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Expenses",
    description="The amount spent by the agency for law enforcement activities.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        members=LawEnforcementExpensesIncludesExcludes,
        excluded_set={
            LawEnforcementExpensesIncludesExcludes.BIENNIUM_FUNDING,
            LawEnforcementExpensesIncludesExcludes.MULTI_YEAR_EXPENSES,
            LawEnforcementExpensesIncludesExcludes.JAILS,
            LawEnforcementExpensesIncludesExcludes.SUPERVISION,
        },
    ),
    specified_contexts=[],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=LawEnforcementExpenseType,
            dimension_to_includes_excludes={
                LawEnforcementExpenseType.TRAINING: IncludesExcludesSet(
                    members=LawEnforcementTrainingIncludesExcludes,
                    excluded_set={
                        LawEnforcementTrainingIncludesExcludes.FREE,
                    },
                ),
                LawEnforcementExpenseType.PERSONNEL: IncludesExcludesSet(
                    members=LawEnforcementPersonnelIncludesExcludes,
                    excluded_set={
                        LawEnforcementPersonnelIncludesExcludes.COMPANY_CONTRACTS,
                    },
                ),
                LawEnforcementExpenseType.FACILITIES_AND_EQUIPMENT: IncludesExcludesSet(
                    members=LawEnforcementFacilitiesIncludesExcludes,
                ),
            },
            dimension_to_description={
                LawEnforcementExpenseType.PERSONNEL: "The amount spent by the agency to employ personnel involved in law enforcement activities.",
                LawEnforcementExpenseType.TRAINING: "The amount spent by the agency on the training of personnel involved in law enforcement activities.",
                LawEnforcementExpenseType.FACILITIES_AND_EQUIPMENT: "The amount spent by the agency for the purchase and use of the physical plant and property owned and operated by the agency and equipment used in law enforcement activities.",
                LawEnforcementExpenseType.OTHER: "The amount spent by the agency on other costs relating to law enforcement activities that are not personnel, training, or facilities and equipment expenses.",
                LawEnforcementExpenseType.UNKNOWN: "The amount spent by the agency on costs relating to law enforcement activities for a purpose that is not known.",
            },
            required=False,
        )
    ],
)

calls_for_service = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.CALLS_FOR_SERVICE,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Calls for Service",
    description="Measures the number of calls for service routed to your agency.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Do not include calls that are officer-initiated.",
    definitions=[
        Definition(
            term="Calls for service",
            definition="One case that represents a request for police service generated by the community and received through an emergency or non-emergency method (911, 311, 988, online report). Count all calls for service, regardless of whether an underlying incident report was filed.",
        )
    ],
    specified_contexts=[
        Context(
            key=ContextKey.ALL_CALLS_OR_CALLS_RESPONDED,
            value_type=ValueType.MULTIPLE_CHOICE,
            label="Does the total value include all calls or just those responded to?",
            required=True,
            multiple_choice_options=CallsRespondedOptions,
        ),
        Context(
            key=ContextKey.AGENCIES_AVAILABLE_FOR_RESPONSE,
            value_type=ValueType.TEXT,
            label="Please list the names of all agencies available for response.",
            required=False,
        ),
    ],
    aggregated_dimensions=[AggregatedDimension(dimension=CallType, required=True)],
)

staff = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    specified_contexts=[],
    includes_excludes=IncludesExcludesSet(
        members=LawEnforcementStaffIncludesExcludes,
        excluded_set={
            LawEnforcementStaffIncludesExcludes.INTERN,
            LawEnforcementStaffIncludesExcludes.VOLUNTEER,
            LawEnforcementStaffIncludesExcludes.NOT_FUNDED,
        },
    ),
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for and paid by the agency for law enforcement activities.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=LawEnforcementStaffType,
            required=False,
            dimension_to_includes_excludes={
                LawEnforcementStaffType.LAW_ENFORCEMENT_OFFICERS: IncludesExcludesSet(
                    members=LawEnforcementPoliceOfficersIncludesExcludes,
                    excluded_set={
                        LawEnforcementPoliceOfficersIncludesExcludes.CRISIS_INTERVENTION,
                        LawEnforcementPoliceOfficersIncludesExcludes.VACANT,
                        LawEnforcementPoliceOfficersIncludesExcludes.VICTIM_ADVOCATE,
                    },
                ),
                LawEnforcementStaffType.CIVILIAN_STAFF: IncludesExcludesSet(
                    members=LawEnforcementCivilianStaffIncludesExcludes,
                ),
                LawEnforcementStaffType.MENTAL_HEALTH: IncludesExcludesSet(
                    members=LawEnforcementMentalHealthStaffIncludesExcludes,
                    excluded_set={
                        LawEnforcementMentalHealthStaffIncludesExcludes.PART_TIME,
                    },
                ),
                LawEnforcementStaffType.VICTIM_ADVOCATES: IncludesExcludesSet(
                    members=LawEnforcementVictimAdvocateStaffIncludesExcludes,
                    excluded_set={
                        LawEnforcementVictimAdvocateStaffIncludesExcludes.PART_TIME,
                    },
                ),
                LawEnforcementStaffType.VACANT: IncludesExcludesSet(
                    members=LawEnforcementVacantStaffIncludesExcludes,
                    excluded_set={
                        LawEnforcementVacantStaffIncludesExcludes.FILLED,
                    },
                ),
            },
            dimension_to_description={
                LawEnforcementStaffType.LAW_ENFORCEMENT_OFFICERS: "The number of full-time equivalent positions that perform law enforcement activities and ordinarily carry a firearm and a badge.",
                LawEnforcementStaffType.CIVILIAN_STAFF: "The number of full-time equivalent positions that work as civilian or non-sworn employees.",
                LawEnforcementStaffType.MENTAL_HEALTH: "The number of full-time equivalent positions that are members of a Crisis Intervention Team or provide mental health services in collaboration with law enforcement.",
                LawEnforcementStaffType.VICTIM_ADVOCATES: "The number of full-time equivalent positions that provide victim support services.",
                LawEnforcementStaffType.OTHER: " The number of full-time equivalent positions budgeted to the law enforcement agency that are not sworn/uniformed police officers, civilian staff, mental health/Crisis Intervention Team staff, or victim advocate staff.",
                LawEnforcementStaffType.UNKNOWN: "The number of full-time equivalent positions budgeted to the law enforcement agency that are of an unknown type.",
                LawEnforcementStaffType.VACANT: "The number of full-time equivalent positions of any type budgeted to the law enforcement agency but not currently filled.",
            },
        ),
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=False,
        ),
        AggregatedDimension(
            dimension=BiologicalSex,
            required=False,
            dimension_to_description={
                BiologicalSex.MALE: "A single day count of the number of people in filled staff positions whose biological sex is male.",
                BiologicalSex.FEMALE: "A single day count of the number of people in filled staff positions whose biological sex is male.",
                BiologicalSex.UNKNOWN: "A single day count of the number of people in filled staff positions whose biological sex is unknown.",
            },
        ),
    ],
)

reported_crime = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.REPORTED_CRIME,
    category=MetricCategory.POPULATIONS,
    display_name="Reported Crime",
    description="Measures the number of crimes reported to your agency.",
    definitions=[
        Definition(
            term="Crime incident",
            definition="One case that represents one or more offenses committed by the same person/group of persons at the same time and place and for which a crime report was filed. Report each incident only once, relying on the FBI hierarchy rule for crime reporting.",
        )
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[AggregatedDimension(dimension=OffenseType, required=True)],
)

total_arrests = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.ARRESTS,
    category=MetricCategory.POPULATIONS,
    display_name="Total Arrests",
    description="Measures the number of arrests made by your agency.",
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_ARREST,
            value_type=ValueType.TEXT,
            label="Please provide your jurisdiction's definition of arrest.",
            required=True,
        )
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(dimension=OffenseType, required=True),
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(dimension=GenderRestricted, required=True),
    ],
)

officer_use_of_force_incidents = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Use of Force Incidents",
    description="Measures the number of use of force incidents.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Select the most serious type of force used per incident.",
    definitions=[
        Definition(
            term="Use of force incident",
            definition="An event in which an officer uses force towards or in the vicinity of a civilian. The FBI focuses on uses of force resulting in injury or a discharge of a weapon.  Count all uses of force occurring during the same event as 1 incident.",
        )
    ],
    specified_contexts=[
        Context(
            key=ContextKey.JURISDICTION_DEFINITION_OF_USE_OF_FORCE,
            value_type=ValueType.TEXT,
            label="Please provide your jurisdiction's definition of use of force.",
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
    display_name="Civilian Complaints Sustained",
    description="Measures the number of complaints filed against officers in your agency that were ultimately sustained.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    definitions=[
        Definition(
            term="Complaint",
            definition="One case that represents one or more acts committed by the same officer, or group of officers at the same time and place. Count all complaints, regardless of whether an underlying incident was filed.",
        ),
        Definition(
            term="Sustained",
            definition="Found to be supported by the evidence, and may or may not result in disciplinary action.",
        ),
    ],
)
