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
)
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.law_enforcement import (
    CallsForServiceEmergencyCallsIncludesExcludes,
    CallsForServiceIncludesExcludes,
    CallsForServiceNonEmergencyCallsIncludesExcludes,
    LawEnforcementArrestsIncludesExcludes,
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
    LawEnforcementReportedCrimeIncludesExcludes,
    LawEnforcementStaffIncludesExcludes,
    LawEnforcementStateAppropriationIncludesExcludes,
    LawEnforcementTrainingIncludesExcludes,
    LawEnforcementVacantStaffIncludesExcludes,
    LawEnforcementVictimAdvocateStaffIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.offense import (
    DrugOffenseIncludesExcludes,
    PersonOffenseIncludesExcludes,
    PropertyOffenseIncludesExcludes,
    PublicOrderOffenseIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.person import (
    FemaleBiologicalSexIncludesExcludes,
    MaleBiologicalSexIncludesExcludes,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
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
    description="The number of calls for police assistance received by the agency.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=CallsForServiceIncludesExcludes,
        excluded_set={
            CallsForServiceIncludesExcludes.FIRE_SERVICE,
            CallsForServiceIncludesExcludes.EMS,
            CallsForServiceIncludesExcludes.NON_POLICE_SERVICE,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=CallType,
            required=True,
            dimension_to_includes_excludes={
                CallType.EMERGENCY: IncludesExcludesSet(
                    members=CallsForServiceEmergencyCallsIncludesExcludes
                ),
                CallType.NON_EMERGENCY: IncludesExcludesSet(
                    members=CallsForServiceNonEmergencyCallsIncludesExcludes
                ),
            },
            dimension_to_description={
                CallType.EMERGENCY: "The number of calls for police assistance received by the agency that require immediate response.",
                CallType.NON_EMERGENCY: "The number of calls for police assistance received by the agency that do not require immediate response.",
                CallType.OTHER: "The number of calls for police assistance received by the agency that are not emergency or non-emergency calls.",
                CallType.UNKNOWN: "The number of calls for police assistance received by the agency of a type that is not known.",
            },
        )
    ],
)

staff = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
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
                LawEnforcementStaffType.OTHER: "The number of full-time equivalent positions budgeted to the law enforcement agency that are not sworn/uniformed police officers, civilian staff, mental health/Crisis Intervention Team staff, or victim advocate staff.",
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
            dimension_to_includes_excludes={
                BiologicalSex.MALE: IncludesExcludesSet(
                    members=MaleBiologicalSexIncludesExcludes,
                    excluded_set={MaleBiologicalSexIncludesExcludes.UNKNOWN},
                ),
                BiologicalSex.FEMALE: IncludesExcludesSet(
                    members=FemaleBiologicalSexIncludesExcludes,
                    excluded_set={FemaleBiologicalSexIncludesExcludes.UNKNOWN},
                ),
                # Unknown Biological Sex has a Y/N Table associated with it.
            },
            dimension_to_description={
                BiologicalSex.MALE: "A single day count of the number of people in filled staff positions whose biological sex is male.",
                BiologicalSex.FEMALE: "A single day count of the number of people in filled staff positions whose biological sex is female.",
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
    includes_excludes=IncludesExcludesSet(
        members=LawEnforcementReportedCrimeIncludesExcludes,
        excluded_set={
            LawEnforcementReportedCrimeIncludesExcludes.REFERRED_TO_OTHER_AGENCY
        },
    ),
    description="The number of criminal incidents made known to the agency.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=True,
            dimension_to_description={
                OffenseType.DRUG: "The number of reported crime incidents received by the agency in which the most serious offense was a drug offense.",
                OffenseType.PERSON: "The number of reported crime incidents received by the agency in which the most serious offense was a crime against a person.",
                OffenseType.PROPERTY: "The number of reported crime incidents received by the agency in which the most serious offense was a property offense.",
                OffenseType.PUBLIC_ORDER: "The number of reported crime incidents received by the agency in which the most serious offense was a public order offense.",
                OffenseType.OTHER: "The number of reported crime incidents received by the agency in which the most serious offense was another type of crime that was not a person, property, drug, or public order offense.",
                OffenseType.UNKNOWN: "The number of reported crime incidents received by the agency in which the most serious offense is not known.",
            },
        )
    ],
)

arrests = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.ARRESTS,
    category=MetricCategory.POPULATIONS,
    display_name="Arrests",
    description="The number of arrests, citations, and summonses made by the agency.",
    measurement_type=MeasurementType.DELTA,
    includes_excludes=IncludesExcludesSet(
        members=LawEnforcementArrestsIncludesExcludes,
        excluded_set={LawEnforcementArrestsIncludesExcludes.OUTSIDE_JURISDICTION},
    ),
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=True,
            dimension_to_description={
                OffenseType.PERSON: "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a crime against a person.",
                OffenseType.PROPERTY: "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a property offense.",
                OffenseType.DRUG: "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a drug offense.",
                OffenseType.PUBLIC_ORDER: "The number of arrests, citations, or summonses made by the agency in which the most serious offense was a public order offense.",
                OffenseType.OTHER: "The number of arrests, citations, or summonses made by the agency in which the most serious offense was another type of crime that was not a person, property, drug, or public order offense.",
                OffenseType.UNKNOWN: "The number of arrests, citations, or summonses made by the agency in which the most serious offense is not known.",
            },
            dimension_to_includes_excludes={
                OffenseType.PERSON: IncludesExcludesSet(
                    members=PersonOffenseIncludesExcludes,
                    excluded_set={PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE},
                ),
                OffenseType.PROPERTY: IncludesExcludesSet(
                    members=PropertyOffenseIncludesExcludes,
                    excluded_set={PropertyOffenseIncludesExcludes.ROBBERY},
                ),
                OffenseType.PUBLIC_ORDER: IncludesExcludesSet(
                    members=PublicOrderOffenseIncludesExcludes,
                    excluded_set={
                        PublicOrderOffenseIncludesExcludes.DRUG_VIOLATIONS,
                        PublicOrderOffenseIncludesExcludes.DRUG_EQUIPMENT_VIOLATIONS,
                        PublicOrderOffenseIncludesExcludes.DRUG_SALES,
                        PublicOrderOffenseIncludesExcludes.DRUG_DISTRIBUTION,
                        PublicOrderOffenseIncludesExcludes.DRUG_MANUFACTURING,
                        PublicOrderOffenseIncludesExcludes.DRUG_SMUGGLING,
                        PublicOrderOffenseIncludesExcludes.DRUG_PRODUCTION,
                        PublicOrderOffenseIncludesExcludes.DRUG_POSSESSION,
                    },
                ),
                OffenseType.DRUG: IncludesExcludesSet(
                    members=DrugOffenseIncludesExcludes
                ),
            },
        ),
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(
            dimension=BiologicalSex,
            required=True,
            dimension_to_description={
                BiologicalSex.MALE: "The number of arrests, citations, and summonses by the agency of people whose biological sex is male.",
                BiologicalSex.FEMALE: "The number of arrests, citations, and summonses by the agency of people whose biological sex is female.",
                BiologicalSex.UNKNOWN: "The number of arrests, citations, and summonses by the agency of people whose biological sex is not known.",
            },
        ),
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
