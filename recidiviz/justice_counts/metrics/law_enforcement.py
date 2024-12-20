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

from recidiviz.justice_counts.dimensions.common import ExpenseType
from recidiviz.justice_counts.dimensions.law_enforcement import (
    CallType,
    ComplaintType,
    ForceType,
    FundingType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    CensusRace,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.law_enforcement import (
    CallsForServiceEmergencyCallsIncludesExcludes,
    CallsForServiceIncludesExcludes,
    CallsForServiceNonEmergencyCallsIncludesExcludes,
    LawEnforcementArrestsIncludesExcludes,
    LawEnforcementAssetForfeitureIncludesExcludes,
    LawEnforcementCivilianComplaintsSustainedIncludesExcludes,
    LawEnforcementCivilianStaffIncludesExcludes,
    LawEnforcementCountyOrMunicipalAppropriation,
    LawEnforcementDiscriminationOrRacialBiasIncludesExcludes,
    LawEnforcementExcessiveUsesOfForceIncludesExcludes,
    LawEnforcementExpensesTimeframeIncludesExcludes,
    LawEnforcementExpensesTypeIncludesExcludes,
    LawEnforcementFacilitiesIncludesExcludes,
    LawEnforcementFirearmIncludesExcludes,
    LawEnforcementFundingPurposeIncludesExcludes,
    LawEnforcementFundingTimeframeIncludesExcludes,
    LawEnforcementGrantsIncludesExcludes,
    LawEnforcementMentalHealthStaffIncludesExcludes,
    LawEnforcementOtherWeaponIncludesExcludes,
    LawEnforcementPersonnelIncludesExcludes,
    LawEnforcementPhysicalForceIncludesExcludes,
    LawEnforcementPoliceOfficersIncludesExcludes,
    LawEnforcementReportedCrimeIncludesExcludes,
    LawEnforcementRestraintIncludesExcludes,
    LawEnforcementStaffIncludesExcludes,
    LawEnforcementStateAppropriationIncludesExcludes,
    LawEnforcementTrainingIncludesExcludes,
    LawEnforcementUseOfForceIncidentsIncludesExcludes,
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
    IncludesExcludesSet,
    MetricCategory,
    MetricDefinition,
    ReportingFrequency,
)
from recidiviz.justice_counts.utils.constants import MetricUnit
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    System,
)

funding = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for agency law enforcement activities.",
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=LawEnforcementFundingTimeframeIncludesExcludes,
            description="Funding timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=LawEnforcementFundingPurposeIncludesExcludes,
            description="Funding purpose",
            excluded_set={
                LawEnforcementFundingPurposeIncludesExcludes.JAIL_OPERATIONS,
                LawEnforcementFundingPurposeIncludesExcludes.SUPERVISION_SERVICES,
                LawEnforcementFundingPurposeIncludesExcludes.JUVENILE_JAIL_OPERATIONS,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATION: [
                    IncludesExcludesSet(
                        members=LawEnforcementStateAppropriationIncludesExcludes,
                        excluded_set={
                            LawEnforcementStateAppropriationIncludesExcludes.PRELIMINARY,
                            LawEnforcementStateAppropriationIncludesExcludes.PROPOSED,
                        },
                    ),
                ],
                FundingType.COUNTY_APPROPRIATION: [
                    IncludesExcludesSet(
                        members=LawEnforcementCountyOrMunicipalAppropriation,
                        excluded_set={
                            LawEnforcementCountyOrMunicipalAppropriation.PRELIMINARY,
                            LawEnforcementCountyOrMunicipalAppropriation.PROPOSED,
                        },
                    ),
                ],
                FundingType.ASSET_FORFEITURE: [
                    IncludesExcludesSet(
                        members=LawEnforcementAssetForfeitureIncludesExcludes,
                    ),
                ],
                FundingType.GRANTS: [
                    IncludesExcludesSet(
                        members=LawEnforcementGrantsIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                FundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for agency law enforcement activities.",
                FundingType.COUNTY_APPROPRIATION: "The amount of funding appropriated by counties or municipalities for agency law enforcement activities.",
                FundingType.ASSET_FORFEITURE: "The amount of funding derived by the agency through the seizure of assets.",
                FundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for agency law enforcement activities.",
                FundingType.OTHER: "The amount of funding to be used for agency law enforcement activities that is not appropriations from the state, appropriations from the county or city, asset forfeiture, or grants.",
                FundingType.UNKNOWN: "The amount of funding to be used for agency law enforcement activities for which the source is not known.",
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
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=LawEnforcementExpensesTimeframeIncludesExcludes,
            description="Expenses timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=LawEnforcementExpensesTypeIncludesExcludes,
            description="Expense type",
            excluded_set={
                LawEnforcementExpensesTypeIncludesExcludes.JAILS,
                LawEnforcementExpensesTypeIncludesExcludes.SUPERVISION,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            dimension_to_includes_excludes={
                ExpenseType.TRAINING: [
                    IncludesExcludesSet(
                        members=LawEnforcementTrainingIncludesExcludes,
                        excluded_set={
                            LawEnforcementTrainingIncludesExcludes.FREE,
                        },
                    ),
                ],
                ExpenseType.PERSONNEL: [
                    IncludesExcludesSet(
                        members=LawEnforcementPersonnelIncludesExcludes,
                        excluded_set={
                            LawEnforcementPersonnelIncludesExcludes.COMPANY_CONTRACTS,
                        },
                    ),
                ],
                ExpenseType.FACILITIES: [
                    IncludesExcludesSet(
                        members=LawEnforcementFacilitiesIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount spent by the agency to employ personnel involved in law enforcement activities.",
                ExpenseType.TRAINING: "The amount spent by the agency on the training of personnel involved in law enforcement activities.",
                ExpenseType.FACILITIES: "The amount spent by the agency for the purchase and use of the physical plant and property owned and operated by the agency and equipment used in law enforcement activities.",
                ExpenseType.OTHER: "The amount spent by the agency on other costs relating to law enforcement activities that are not personnel, training, or facilities and equipment expenses.",
                ExpenseType.UNKNOWN: "The amount spent by the agency on costs relating to law enforcement activities for a purpose that is not known.",
            },
            required=False,
        )
    ],
)

calls_for_service = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.CALLS_FOR_SERVICE,
    category=MetricCategory.POPULATIONS,
    display_name="Calls for Service",
    description="The number of calls for police assistance received by the agency.",
    unit=MetricUnit.CALLS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=CallsForServiceIncludesExcludes,
            excluded_set={
                CallsForServiceIncludesExcludes.FIRE_SERVICE,
                CallsForServiceIncludesExcludes.EMS,
                CallsForServiceIncludesExcludes.NON_POLICE_SERVICE,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=CallType,
            required=True,
            dimension_to_includes_excludes={
                CallType.EMERGENCY: [
                    IncludesExcludesSet(
                        members=CallsForServiceEmergencyCallsIncludesExcludes
                    ),
                ],
                CallType.NON_EMERGENCY: [
                    IncludesExcludesSet(
                        members=CallsForServiceNonEmergencyCallsIncludesExcludes
                    ),
                ],
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
    includes_excludes=[
        IncludesExcludesSet(
            members=LawEnforcementStaffIncludesExcludes,
            excluded_set={
                LawEnforcementStaffIncludesExcludes.INTERN,
                LawEnforcementStaffIncludesExcludes.VOLUNTEER,
                LawEnforcementStaffIncludesExcludes.NOT_FUNDED,
            },
        ),
    ],
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for and paid by the agency for law enforcement activities.",
    additional_description="Staff positions should only be counted once per full-time equivalent (FTE). If one FTE position has job functions that span more than one type of role, please count that FTE position in the role with the largest percentage of job functions.",
    unit=MetricUnit.FULL_TIME,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_includes_excludes={
                StaffType.LAW_ENFORCEMENT_OFFICERS: [
                    IncludesExcludesSet(
                        members=LawEnforcementPoliceOfficersIncludesExcludes,
                        excluded_set={
                            LawEnforcementPoliceOfficersIncludesExcludes.CRISIS_INTERVENTION,
                            LawEnforcementPoliceOfficersIncludesExcludes.VACANT,
                            LawEnforcementPoliceOfficersIncludesExcludes.VICTIM_ADVOCATE,
                        },
                    ),
                ],
                StaffType.CIVILIAN_STAFF: [
                    IncludesExcludesSet(
                        members=LawEnforcementCivilianStaffIncludesExcludes,
                    ),
                ],
                StaffType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=LawEnforcementMentalHealthStaffIncludesExcludes,
                        excluded_set={
                            LawEnforcementMentalHealthStaffIncludesExcludes.PART_TIME,
                        },
                    ),
                ],
                StaffType.VICTIM_ADVOCATES: [
                    IncludesExcludesSet(
                        members=LawEnforcementVictimAdvocateStaffIncludesExcludes,
                        excluded_set={
                            LawEnforcementVictimAdvocateStaffIncludesExcludes.PART_TIME,
                        },
                    ),
                ],
                StaffType.VACANT: [
                    IncludesExcludesSet(
                        members=LawEnforcementVacantStaffIncludesExcludes,
                        excluded_set={
                            LawEnforcementVacantStaffIncludesExcludes.FILLED,
                        },
                    ),
                ],
            },
            dimension_to_description={
                StaffType.LAW_ENFORCEMENT_OFFICERS: "The number of full-time equivalent positions that perform law enforcement activities and ordinarily carry a firearm and a badge.",
                StaffType.CIVILIAN_STAFF: "The number of full-time equivalent positions that work as civilian or non-sworn employees.",
                StaffType.MENTAL_HEALTH: "The number of full-time equivalent positions that are members of a Crisis Intervention Team or provide mental health services in collaboration with law enforcement.",
                StaffType.VICTIM_ADVOCATES: "The number of full-time equivalent positions that provide victim support services.",
                StaffType.OTHER: " The number of full-time equivalent positions budgeted to the law enforcement agency that are not sworn/uniformed police officers, civilian staff, mental health/Crisis Intervention Team staff, or victim advocate staff.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions budgeted to the law enforcement agency that are of an unknown type.",
                StaffType.VACANT: "The number of full-time equivalent positions of any type budgeted to the law enforcement agency but not currently filled.",
            },
        ),
        # TODO(#18071) Replace this with reference to Global Includes/Excludes once those are implemented
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=False,
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "The number of staff in filled positions whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "The number of staff in filled positions whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "The number of staff in filled positions whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.MORE_THAN_ONE_RACE: "The number of staff in filled positions whose race is listed as more than one race, such as White and Black.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "The number of staff in filled positions whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "The number of staff in filled positions whose race is listed as some other race, not included above.",
                CensusRace.WHITE: "The number of staff in filled positions whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
                CensusRace.UNKNOWN: "The number of staff in filled positions whose race is not known.",
                CensusRace.HISPANIC_OR_LATINO: "The number of staff in filled positions whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
            },
        ),
        # TODO(#18071) Replace this with reference to Global Includes/Excludes once those are implemented
        AggregatedDimension(
            dimension=BiologicalSex,
            required=False,
            dimension_to_includes_excludes={
                BiologicalSex.MALE: [
                    IncludesExcludesSet(
                        members=MaleBiologicalSexIncludesExcludes,
                        excluded_set={MaleBiologicalSexIncludesExcludes.UNKNOWN},
                    ),
                ],
                BiologicalSex.FEMALE: [
                    IncludesExcludesSet(
                        members=FemaleBiologicalSexIncludesExcludes,
                        excluded_set={FemaleBiologicalSexIncludesExcludes.UNKNOWN},
                    ),
                ],
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
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Reported Crime",
    includes_excludes=[
        IncludesExcludesSet(
            members=LawEnforcementReportedCrimeIncludesExcludes,
            excluded_set={
                LawEnforcementReportedCrimeIncludesExcludes.REFERRED_TO_OTHER_AGENCY
            },
        ),
    ],
    description="The number of criminal incidents made known to the agency.",
    additional_description="The reported crime metric is based on the number of criminal incidents reported to the agency, not the number of offenses, victims, or people accused of committing a crime. An incident is one or more offenses committed by the same person, or a group of people acting in concert, at the same time and place. If more than one offense occurs within an incident, only the most serious offense is counted. Data should be categorized by the most severe offense in the incident. Justice Counts recommends following the summary reporting model of crimes against persons considered most serious, followed by crimes against property, public order offenses, drug offenses, other offenses, and unknown offenses. If more than one person was involved in the incident, it is only counted once.",
    unit=MetricUnit.REPORTED_INCIDENTS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) Replace this with reference to Global Includes/Excludes once those are implemented
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=True,
            dimension_to_includes_excludes={
                OffenseType.PERSON: [
                    IncludesExcludesSet(
                        members=PersonOffenseIncludesExcludes,
                        excluded_set={
                            PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE
                        },
                    ),
                ],
                OffenseType.PROPERTY: [
                    IncludesExcludesSet(
                        members=PropertyOffenseIncludesExcludes,
                        excluded_set={PropertyOffenseIncludesExcludes.ROBBERY},
                    ),
                ],
                OffenseType.PUBLIC_ORDER: [
                    IncludesExcludesSet(
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
                ],
                OffenseType.DRUG: [
                    IncludesExcludesSet(members=DrugOffenseIncludesExcludes),
                ],
            },
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
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Arrests",
    description="The number of arrests, citations, and summonses made by the agency.",
    additional_description="Arrests are based on the number of arrest events, not the number of unique people arrested. If the same person was arrested three times during a time period, it would count as three arrests. A person suspected of committing more than one offense, but arrested only once, should be counted as one arrest classified by the most serious charge in the incident. Justice Counts recommends following the summary reporting model of crimes against persons considered most serious, followed by crimes against property, public order offenses, drug offenses, other offenses, and unknown offenses. Law enforcement agencies should only share data for those arrests made for offenses committed within their own jurisdictions.",
    unit=MetricUnit.ARRESTS,
    measurement_type=MeasurementType.DELTA,
    includes_excludes=[
        IncludesExcludesSet(
            members=LawEnforcementArrestsIncludesExcludes,
            excluded_set={LawEnforcementArrestsIncludesExcludes.OUTSIDE_JURISDICTION},
        ),
    ],
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) Replace this with reference to Global Includes/Excludes once those are implemented
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
                OffenseType.PERSON: [
                    IncludesExcludesSet(
                        members=PersonOffenseIncludesExcludes,
                        excluded_set={
                            PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE
                        },
                    ),
                ],
                OffenseType.PROPERTY: [
                    IncludesExcludesSet(
                        members=PropertyOffenseIncludesExcludes,
                        excluded_set={PropertyOffenseIncludesExcludes.ROBBERY},
                    ),
                ],
                OffenseType.PUBLIC_ORDER: [
                    IncludesExcludesSet(
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
                ],
                OffenseType.DRUG: [
                    IncludesExcludesSet(members=DrugOffenseIncludesExcludes),
                ],
            },
        ),
        # TODO(#18071) Replace this with reference to Global Includes/Excludes once those are implemented
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=True,
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.MORE_THAN_ONE_RACE: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as more than one race, such as White and Black.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as some other race, not included above.",
                CensusRace.WHITE: "The number of arrests, citations, and summonses made by the agency of people whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
                CensusRace.UNKNOWN: "The number of arrests, citations, and summonses made by the agency of people whose race is not known.",
                CensusRace.HISPANIC_OR_LATINO: "The number of arrests, citations, and summonses made by the agency of people whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
            },
        ),
        # TODO(#18071) Replace this with reference to Global Includes/Excludes once those are implemented
        AggregatedDimension(
            dimension=BiologicalSex,
            required=True,
            dimension_to_includes_excludes={
                BiologicalSex.MALE: [
                    IncludesExcludesSet(
                        members=MaleBiologicalSexIncludesExcludes,
                        excluded_set={
                            MaleBiologicalSexIncludesExcludes.UNKNOWN,
                        },
                    ),
                ],
                BiologicalSex.FEMALE: [
                    IncludesExcludesSet(
                        members=FemaleBiologicalSexIncludesExcludes,
                        excluded_set={
                            FemaleBiologicalSexIncludesExcludes.UNKNOWN,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BiologicalSex.MALE: "The number of arrests, citations, and summonses by the agency of people whose biological sex is male.",
                BiologicalSex.FEMALE: "The number of arrests, citations, and summonses by the agency of people whose biological sex is female.",
                BiologicalSex.UNKNOWN: "The number of arrests, citations, and summonses by the agency of people whose biological sex is not known.",
            },
        ),
    ],
)

use_of_force_incidents = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Use of Force Incidents",
    description="The number of incidents in which agency staff used physical coercion to gain compliance from a person.",
    additional_description="Use of force incidents are based on the number of unique incidents where force was used, not the number of individual people or staff involved in those incidents. If one agency staff member used force to gain compliance from a person three times in a time period, it would count as three use of force incidents.",
    unit=MetricUnit.INCIDENTS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=LawEnforcementUseOfForceIncidentsIncludesExcludes,
            excluded_set={
                LawEnforcementUseOfForceIncidentsIncludesExcludes.NOT_INTENTED_INJURY,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ForceType,
            dimension_to_includes_excludes={
                ForceType.PHYSICAL: [
                    IncludesExcludesSet(
                        members=LawEnforcementPhysicalForceIncludesExcludes,
                        excluded_set={
                            LawEnforcementPhysicalForceIncludesExcludes.WITHOUT_SUFFICIENT_FORCE,
                        },
                    ),
                ],
                ForceType.RESTRAINT: [
                    IncludesExcludesSet(
                        members=LawEnforcementRestraintIncludesExcludes,
                    ),
                ],
                ForceType.FIREARM: [
                    IncludesExcludesSet(
                        members=LawEnforcementFirearmIncludesExcludes,
                        excluded_set={
                            LawEnforcementFirearmIncludesExcludes.DISPLAYING_FIREARM,
                        },
                    ),
                ],
                ForceType.OTHER_WEAPON: [
                    IncludesExcludesSet(
                        members=LawEnforcementOtherWeaponIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                ForceType.PHYSICAL: "The number of incidents in which agency staff used physical force to gain compliance from a person.",
                ForceType.FIREARM: "The number of incidents in which agency staff used a firearm to gain compliance from a person.",
                ForceType.RESTRAINT: "The number of incidents in which agency staff used a restraint to gain compliance from a person.",
                ForceType.OTHER_WEAPON: "The number of incidents in which agency staff used a non-firearm weapon to gain compliance from a person.",
                ForceType.OTHER: "The number of incidents in which agency staff used another type of force to gain compliance from a person.",
                ForceType.UNKNOWN: "The number of incidents in which agency staff used an unknown type of force to gain compliance from a person.",
            },
            required=True,
        )
    ],
)

civilian_complaints_sustained = MetricDefinition(
    system=System.LAW_ENFORCEMENT,
    metric_type=MetricType.COMPLAINTS_SUSTAINED,
    category=MetricCategory.FAIRNESS,
    display_name="Civilian Complaints Sustained",
    description="The number of allegations of misconduct filed against agency staff that were sustained by an internal affairs unit or review board.",
    additional_description="The civilian complaints sustained metric is based on the number of staff members investigated and sanctioned. If one complaint is related to three staff members, it should be counted as three complaints.",
    unit=MetricUnit.COMPLAINTS_SUSTAINED,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=LawEnforcementCivilianComplaintsSustainedIncludesExcludes,
            excluded_set={
                LawEnforcementCivilianComplaintsSustainedIncludesExcludes.NOT_SUSTAINED,
                LawEnforcementCivilianComplaintsSustainedIncludesExcludes.UNFOUNDED,
                LawEnforcementCivilianComplaintsSustainedIncludesExcludes.POLICY_VIOLATION,
                LawEnforcementCivilianComplaintsSustainedIncludesExcludes.LAWFUL,
                LawEnforcementCivilianComplaintsSustainedIncludesExcludes.NOT_RESOLVED,
                LawEnforcementCivilianComplaintsSustainedIncludesExcludes.INFORMAL,
                LawEnforcementCivilianComplaintsSustainedIncludesExcludes.DUPLICATE,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ComplaintType,
            required=False,
            dimension_to_includes_excludes={
                ComplaintType.EXCESSIVE_USES_OF_FORCE: [
                    IncludesExcludesSet(
                        members=LawEnforcementExcessiveUsesOfForceIncludesExcludes,
                    ),
                ],
                ComplaintType.DISCRIMINATION: [
                    IncludesExcludesSet(
                        members=LawEnforcementDiscriminationOrRacialBiasIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                ComplaintType.EXCESSIVE_USES_OF_FORCE: "The number of allegations of misconduct filed against agency staff relating to excessive uses of force that were sustained by an internal affairs unit or conduct review board.",
                ComplaintType.DISCRIMINATION: "The number of allegations of misconduct filed against agency staff relating to discrimination or racial bias that were sustained by an internal affairs unit or review board.",
                ComplaintType.OTHER: "The number of allegations of misconduct filed against agency staff that were sustained by an internal affairs unit or review board and were not excessive uses of force or discrimination or racial bias.",
                ComplaintType.UNKNOWN: "The number of allegations of misconduct filed against agency staff of an unknown type that were sustained by an internal affairs unit or review board.",
            },
        )
    ],
)
