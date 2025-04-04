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
from recidiviz.common.constants.justice_counts import ContextKey
from recidiviz.justice_counts.dimensions.jails import (
    BehavioralHealthNeedType,
    ExpenseType,
    FundingType,
    GrievancesUpheldType,
    PostAdjudicationReleaseType,
    PreAdjudicationReleaseType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    CensusRace,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.common import (
    CountyOrMunicipalAppropriationIncludesExcludes,
    GrantsIncludesExcludes,
    PostAdjudicationJailPopulation,
    PreAdjudicationJailPopulation,
    StateAppropriationIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.jails import (
    AccessToHealthCareIncludesExcludes,
    ClinicalAndMedicalStaffIncludesExcludes,
    CommissaryAndFeesIncludesExcludes,
    ContractBedsExpensesIncludesExcludes,
    ContractBedsFundingIncludesExcludes,
    CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
    DiscriminationRacialBiasReligiousIncludesExcludes,
    ExpensesTimeframeIncludesExcludes,
    ExpensesTypeIncludesExcludes,
    FacilitiesAndEquipmentIncludesExcludes,
    FundingPurposeIncludesExcludes,
    FundingTimeframeIncludesExcludes,
    GrievancesUpheldIncludesExcludes,
    HealthCareForPeopleWhoAreIncarceratedIncludesExcludes,
    LegalIncludesExcludes,
    LivingConditionsIncludesExcludes,
    ManagementAndOperationsStaffIncludesExcludes,
    MentalHealthNeedIncludesExcludes,
    NoBehavioralHealthNeedIncludesExcludes,
    OtherBehavioralHealthNeedIncludesExcludes,
    PersonalSafetyIncludesExcludes,
    PersonnelIncludesExcludes,
    PostAdjudicationReleasesDueToDeathIncludesExcludes,
    PostAdjudicationReleasesDueToEscapeOrAWOLIncludesExcludes,
    PostAdjudicationReleasesIncludesExcludes,
    PostAdjudicationReleasesNoAdditionalCorrectionalControlIncludesExcludes,
    PostAdjudicationReleasesOtherCommunitySupervisionIncludesExcludes,
    PostAdjudicationReleasesParoleSupervisionIncludesExcludes,
    PostAdjudicationReleasesProbationSupervisionIncludesExcludes,
    PreAdjudicationAdmissionsIncludesExcludes,
    PreAdjudicationReleasesDeathIncludesExcludes,
    PreAdjudicationReleasesEscapeOrAWOLIncludesExcludes,
    PreAdjudicationReleasesIncludesExcludes,
    PreAdjudicationReleasesMonetaryBailIncludesExcludes,
    PreAdjudicationReleasesOwnRecognizanceAwaitingTrialIncludesExcludes,
    ProgrammaticStaffIncludesExcludes,
    SecurityStaffIncludesExcludes,
    StaffIncludesExcludes,
    SubstanceUseNeedIncludesExcludes,
    TotalAdmissionsIncludesExcludes,
    TotalDailyPopulationIncludesExcludes,
    TotalReleasesIncludesExcludes,
    TrainingIncludesExcludes,
    UnknownBehavioralHealthNeedIncludesExcludes,
    UseOfForceIncidentsIncludesExcludes,
    VacantPositionsIncludesExcludes,
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
    IncludesExcludesSet,
    MetricCategory,
    MetricDefinition,
)
from recidiviz.justice_counts.utils.constants import MetricUnit
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    ReportingFrequency,
    System,
)

funding = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    unit=MetricUnit.AMOUNT,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=FundingTimeframeIncludesExcludes,
            description="Funding timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=FundingPurposeIncludesExcludes,
            description="Funding purpose",
            excluded_set={
                FundingPurposeIncludesExcludes.OPERATIONS_MAINTENANCE,
                FundingPurposeIncludesExcludes.JUVENILE,
                FundingPurposeIncludesExcludes.NON_JAIL_ACTIVITIES,
                FundingPurposeIncludesExcludes.LAW_ENFORCEMENT,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=False,
            dimension_to_description={
                FundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for the operation and maintenance of jail facilities and the care of people who are incarcerated in jail under the jurisdiction of the agency.",
                FundingType.COUNTY_MUNICIPAL: "The amount of funding counties or municipalities appropriated for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                FundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                FundingType.COMMISSARY_FEES: "The amount of funding the agency collected through sales and/or fees charged to people who are incarcerated under the jurisdiction of the agency or their visitors.",
                FundingType.CONTRACT_BEDS: "The amount of funding collected by the agency through contracts or per diem payment agreements to provide custody and care for people who are incarcerated under the jurisdiction of another agency.",
                FundingType.OTHER: "The amount of funding for the operation and maintenance of jail facilities and the care of people who are incarcerated that is not a state appropriation, a county or municipal appropriation, grant funding, commissary and fees, or contracted beds.",
                FundingType.UNKNOWN: "The amount of funding for the operation and maintenance of jail facilities and the care of people who are incarcerated for which the source is not known.",
            },
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATION: [
                    IncludesExcludesSet(
                        members=StateAppropriationIncludesExcludes,
                        excluded_set={
                            StateAppropriationIncludesExcludes.PROPOSED,
                            StateAppropriationIncludesExcludes.PRELIMINARY,
                            StateAppropriationIncludesExcludes.GRANTS,
                        },
                    ),
                ],
                FundingType.COUNTY_MUNICIPAL: [
                    IncludesExcludesSet(
                        members=CountyOrMunicipalAppropriationIncludesExcludes,
                        excluded_set={
                            CountyOrMunicipalAppropriationIncludesExcludes.PROPOSED,
                            CountyOrMunicipalAppropriationIncludesExcludes.PRELIMINARY,
                        },
                    ),
                ],
                FundingType.GRANTS: [
                    IncludesExcludesSet(
                        members=GrantsIncludesExcludes,
                    ),
                ],
                FundingType.COMMISSARY_FEES: [
                    IncludesExcludesSet(
                        members=CommissaryAndFeesIncludesExcludes,
                    ),
                ],
                FundingType.CONTRACT_BEDS: [
                    IncludesExcludesSet(
                        members=ContractBedsFundingIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)

expenses = MetricDefinition(
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    system=System.JAILS,
    description="The amount the agency spent for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    unit=MetricUnit.AMOUNT,
    metric_type=MetricType.EXPENSES,
    display_name="Expenses",
    measurement_type=MeasurementType.DELTA,
    category=MetricCategory.CAPACITY_AND_COST,
    includes_excludes=[
        IncludesExcludesSet(
            members=ExpensesTimeframeIncludesExcludes,
            description="Expenses timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=ExpensesTypeIncludesExcludes,
            description="Expense type",
            excluded_set={
                ExpensesTypeIncludesExcludes.OPERATIONS_MAINTENANCE,
                ExpensesTypeIncludesExcludes.JUVENILE,
                ExpensesTypeIncludesExcludes.NON_JAIL_ACTIVITIES,
                ExpensesTypeIncludesExcludes.LAW_ENFORCEMENT,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            required=False,
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount the agency spent to employ personnel involved in the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.TRAINING: "The amount spent by the agency on training personnel involved in the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.FACILITIES: "The amount spent by the agency for the purchase and use of the physical plant and property owned and operated by the agency and equipment used to support maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.HEALTH_CARE: "The amount spent by the agency on medical care for people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.CONTRACT_BEDS: "The amount spent by the agency on contracts with other agencies to provide custody and care for people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.OTHER: "The amount spent by the agency on other costs relating to the operation and maintenance of jail facilities and the care of people who are incarcerated that are not personnel, training, facilities and equipment, health care for people who are incarcerated, or contract beds.",
                ExpenseType.UNKNOWN: "The amount spent by the agency on other costs relating to the operation and maintenance of jail facilities and the care of people who are incarcerated for a purpose that is not known.",
            },
            dimension_to_includes_excludes={
                ExpenseType.PERSONNEL: [
                    IncludesExcludesSet(
                        members=PersonnelIncludesExcludes,
                        excluded_set={
                            PersonnelIncludesExcludes.COMPANIES_CONTRACTED,
                        },
                    ),
                ],
                ExpenseType.TRAINING: [
                    IncludesExcludesSet(
                        members=TrainingIncludesExcludes,
                        excluded_set={
                            TrainingIncludesExcludes.NO_COST_PROGRAMS,
                        },
                    ),
                ],
                ExpenseType.FACILITIES: [
                    IncludesExcludesSet(
                        members=FacilitiesAndEquipmentIncludesExcludes,
                    ),
                ],
                ExpenseType.HEALTH_CARE: [
                    IncludesExcludesSet(
                        members=HealthCareForPeopleWhoAreIncarceratedIncludesExcludes,
                        excluded_set={
                            HealthCareForPeopleWhoAreIncarceratedIncludesExcludes.TRANSPORT_COSTS,
                        },
                    ),
                ],
                ExpenseType.CONTRACT_BEDS: [
                    IncludesExcludesSet(
                        members=ContractBedsExpensesIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)

total_staff = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent (FTE) positions budgeted and paid for by the agency for the operation and maintenance of the jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    additional_description="Staff positions should only be counted once per FTE. If one FTE position has job functions that span more than one type of role, please count that FTE position in the role with the largest percentage of job functions.",
    unit=MetricUnit.FULL_TIME,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=StaffIncludesExcludes,
            excluded_set={
                StaffIncludesExcludes.VOLUNTEER,
                StaffIncludesExcludes.INTERN,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_includes_excludes={
                StaffType.SECURITY: [
                    IncludesExcludesSet(
                        members=SecurityStaffIncludesExcludes,
                        excluded_set={
                            SecurityStaffIncludesExcludes.VACANT,
                        },
                    ),
                ],
                StaffType.MANAGEMENT_AND_OPERATIONS: [
                    IncludesExcludesSet(
                        members=ManagementAndOperationsStaffIncludesExcludes,
                        excluded_set={
                            ManagementAndOperationsStaffIncludesExcludes.VACANT,
                        },
                    ),
                ],
                StaffType.CLINICAL_AND_MEDICAL: [
                    IncludesExcludesSet(
                        members=ClinicalAndMedicalStaffIncludesExcludes,
                        excluded_set={
                            ClinicalAndMedicalStaffIncludesExcludes.VACANT,
                        },
                    ),
                ],
                StaffType.PROGRAMMATIC: [
                    IncludesExcludesSet(
                        members=ProgrammaticStaffIncludesExcludes,
                        excluded_set={
                            ProgrammaticStaffIncludesExcludes.VACANT,
                        },
                    ),
                ],
                StaffType.VACANT: [
                    IncludesExcludesSet(
                        members=VacantPositionsIncludesExcludes,
                        excluded_set={
                            VacantPositionsIncludesExcludes.FILLED,
                        },
                    ),
                ],
            },
            dimension_to_description={
                StaffType.SECURITY: "The number of full-time equivalent positions that work directly with people who are incarcerated and are responsible for their custody, supervision, and monitoring.",
                StaffType.MANAGEMENT_AND_OPERATIONS: "The number of full-time equivalent positions that do not work directly with people who are incarcerated but support the day-to-day operations of the agency.",
                StaffType.CLINICAL_AND_MEDICAL: "The number of full-time equivalent positions that work directly with people who are incarcerated and are responsible for their health.",
                StaffType.PROGRAMMATIC: "The number of full-time equivalent positions that provide services and programming to people who are incarcerated but are not medical or clinical staff.",
                StaffType.OTHER: "The number of full-time equivalent positions dedicated to the operation and maintenance of jail facilities under the jurisdiction of the agency that are not security staff, management and operations staff, clinical and medical staff, or programmatic staff.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions dedicated to the operation and maintenance of jail facilities under the jurisdiction of the agency that are of an unknown type.",
                StaffType.VACANT: "The number of full-time equivalent positions dedicated to operation and maintenance of the jail facilities and the care of people who are incarcerated under the jurisdiction of the agency of any type that are budgeted but not currently filled.",
            },
        )
    ],
)

readmissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.READMISSIONS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Readmissions",
    description="The number of admission events to the agency’s jurisdiction of people who were incarcerated in the agency’s jurisdiction within the previous year (365 days).",
    additional_description="For instance, if a person admitted on June 23, 2022, had been incarcerated at any time between June 23, 2021, and June 23, 2022, it would be counted as a readmission. This metric is based on admission events, so if a person is admitted four times in the time period, that would count as one admission and three readmissions. To state it another way, agencies should count the number of times each person has been admitted in the last 365 days and remove anyone with one admission from the count.",
    unit=MetricUnit.READMISSIONS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    # TODO(#18071) implement reused includes/excludes
)

total_admissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.TOTAL_ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Total Admissions",
    description="The number of admission events to the agency’s jurisdiction.",
    additional_description="Admissions are based on the number of events in which a person was incarcerated in a jail facility, not the number of individual people who entered the facility. If the same person was admitted to jail three times in a time period, it would count as three admissions.",
    unit=MetricUnit.ADMISSIONS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=TotalAdmissionsIncludesExcludes,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_description={
                OffenseType.PERSON: "The total number of admission events in which the most serious charge was for an offense against a person.",
                OffenseType.PROPERTY: "The total number of admission events in which the most serious charge was for a property offense.",
                OffenseType.PUBLIC_ORDER: "The total number of admission events in which the most serious charge was for a public order offense.",
                OffenseType.DRUG: "The total number of admission events in which the most serious charge was for a drug offense.",
                OffenseType.OTHER: "The total number of admission events in which the most serious charge was for another type of offense that was not a person, property, public order, or drug offense.",
                OffenseType.UNKNOWN: "The total number of admission events in which the most serious offense charge type is not known.",
            },
        ),
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        ),
    ],
)

pre_adjudication_admissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.PRE_ADJUDICATION_ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Pre-adjudication Admissions",
    description="The number of admission events to the agency’s jurisdiction in which the person has not yet been adjudicated.",
    additional_description="Pre-adjudication admissions are based on the number of events in which a person was incarcerated in a jail facility, not the number of individual people who entered the facility. If the same person was admitted to jail three times in a time period, it would count as three admissions.",
    unit=MetricUnit.ADMISSIONS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) implement reused includes/excludes
    includes_excludes=[
        IncludesExcludesSet(
            members=PreAdjudicationAdmissionsIncludesExcludes,
            excluded_set={
                PreAdjudicationAdmissionsIncludesExcludes.TEMPORARY_ABSENCE,
                PreAdjudicationAdmissionsIncludesExcludes.MOVING,
            },
        ),
    ],
    # TODO(#18071) implement reused includes/excludes
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_includes_excludes={
                OffenseType.PERSON: [
                    IncludesExcludesSet(
                        members=PersonOffenseIncludesExcludes,
                        excluded_set={
                            PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                        },
                    ),
                ],
                OffenseType.PROPERTY: [
                    IncludesExcludesSet(
                        members=PropertyOffenseIncludesExcludes,
                        excluded_set={
                            PropertyOffenseIncludesExcludes.ROBBERY,
                        },
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
                    IncludesExcludesSet(
                        members=DrugOffenseIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                OffenseType.PERSON: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was an offense against a person.",
                OffenseType.PROPERTY: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was a property offense.",
                OffenseType.PUBLIC_ORDER: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was a public order offense.",
                OffenseType.DRUG: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was a drug offense.",
                OffenseType.OTHER: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was not a person, property, drug, or public order offense.",
                OffenseType.UNKNOWN: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge type is not known.",
            },
        ),
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        ),
    ],
)

post_adjudication_admissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.POST_ADJUDICATION_ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Post-adjudication Admissions",
    description="The number of admission events to the agency’s jurisdiction in which the person has been adjudicated.",
    additional_description="Post-adjudication admissions are based on the number of events in which a person was incarcerated in a jail facility, not the number of individual people who entered the facility. If the same person was admitted to jail three times in a time period, it would count as three admissions.",
    unit=MetricUnit.ADMISSIONS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) implement reused includes/excludes
    includes_excludes=[
        IncludesExcludesSet(
            members=PostAdjudicationJailPopulation,
            excluded_set={
                PostAdjudicationJailPopulation.AWAITING_ARRAIGNMENT,
                PostAdjudicationJailPopulation.UNPAID_BAIL,
                PostAdjudicationJailPopulation.DENIAL_OF_BAIL,
                PostAdjudicationJailPopulation.REVOCATION_OF_BAIL,
                PostAdjudicationJailPopulation.PENDING_ASSESSMENT,
                PostAdjudicationJailPopulation.TRANSFERRED_TO_HOSPITAL,
                PostAdjudicationJailPopulation.PENDING_OUTCOME,
                PostAdjudicationJailPopulation.REVOCATION_PRETRIAL_RELEASE,
                PostAdjudicationJailPopulation.PRETRIAL_SUPERVISION_SANCTION,
                PostAdjudicationJailPopulation.US_MARSHALS_SERVICE,
                PostAdjudicationJailPopulation.TRIBAL_NATION,
                PostAdjudicationJailPopulation.FAILURE_TO_APPEAR,
                PostAdjudicationJailPopulation.FAILURE_TO_PAY,
                PostAdjudicationJailPopulation.HELD_FOR_OTHER_STATE,
            },
        ),
    ],
    # TODO(#18071) implement reused includes/excludes
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_includes_excludes={
                OffenseType.PERSON: [
                    IncludesExcludesSet(
                        members=PersonOffenseIncludesExcludes,
                        excluded_set={
                            PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                        },
                    ),
                ],
                OffenseType.PROPERTY: [
                    IncludesExcludesSet(
                        members=PropertyOffenseIncludesExcludes,
                        excluded_set={
                            PropertyOffenseIncludesExcludes.ROBBERY,
                        },
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
                    IncludesExcludesSet(
                        members=DrugOffenseIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                OffenseType.PERSON: "The number of post-adjudication admission events in which the most serious offense was for an offense against a person.",
                OffenseType.PROPERTY: "The number of post-adjudication admission events in which the most serious offense was for a property offense.",
                OffenseType.PUBLIC_ORDER: "The number of post-adjudication admission events in which the most serious offense was a public order offense.",
                OffenseType.DRUG: "The number of post-adjudication admission events in which the most serious offense was a drug offense.",
                OffenseType.OTHER: "The number of post-adjudication admission events in which the most serious offense was for another type of offense that was not a person, property, drug, or public order offense.",
                OffenseType.UNKNOWN: "The number of post-adjudication admission events in which the most serious offense charge type is not known.",
            },
        ),
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        ),
    ],
)

total_daily_population = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.TOTAL_POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Total Daily Population",
    description="A single day count of the total number of people incarcerated under the agency’s jurisdiction.",
    unit=MetricUnit.PEOPLE_INCARCERATED,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=TotalDailyPopulationIncludesExcludes,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_description={
                OffenseType.PERSON: "A single day count of the total number of people incarcerated under the agency’s jurisdiction in which the most serious charge was for an offense against a person.",
                OffenseType.PROPERTY: "A single day count of the total number of people incarcerated under the agency’s jurisdiction in which the most serious charge was for a property offense.",
                OffenseType.PUBLIC_ORDER: "A single day count of the total number of people incarcerated under the agency’s jurisdiction in which the most serious charge was for a public order offense.",
                OffenseType.DRUG: "A single day count of the total number of people incarcerated under the agency's jurisdiction in which the most serious charge was for a drug offense.",
                OffenseType.OTHER: "A single day count of the total number of people incarcerated under the agency’s jurisdiction in which the most serious charge was for another type of offense that was not a person, property, public order, or drug offense.",
                OffenseType.UNKNOWN: "A single day count of the total number of people incarcerated under the agency’s jurisdiction in which the most serious offense charge type is not known.",
            },
        ),
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=False,
            contexts=[Context(key=ContextKey.OTHER_RACE_DESCRIPTION, label="")],
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.HISPANIC_OR_LATINO: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                CensusRace.MORE_THAN_ONE_RACE: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race is listed as more than one race, such as White and Black.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race is listed as some other race, not included above.",
                CensusRace.UNKNOWN: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race is not known.",
                CensusRace.WHITE: "A single-day count of the number of people incarcerated in the agency's jurisdiction whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
            },
        ),
        AggregatedDimension(
            dimension=BiologicalSex,
            required=False,
        ),
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        ),
    ],
)

pre_adjudication_daily_population = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.PRE_ADJUDICATION_POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Pre-adjudication Daily Population",
    description="A single day count of the number of people incarcerated in the agency’s jurisdiction who have not yet been adjudicated.",
    unit=MetricUnit.PEOPLE_INCARCERATED,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) Implement reused global includes/excludes
    includes_excludes=[
        IncludesExcludesSet(
            members=PreAdjudicationJailPopulation,
            excluded_set={
                PreAdjudicationJailPopulation.SERVE_SENTENCE,
                PreAdjudicationJailPopulation.SPLIT_SENTENCE,
                PreAdjudicationJailPopulation.SUSPEND_SENTENCE,
                PreAdjudicationJailPopulation.REVOCATION_COMMUNITY_SUPERVISION,
                PreAdjudicationJailPopulation.COMMUNITY_SUPERVISION_SANCTION,
                PreAdjudicationJailPopulation.COURT_SANCTION,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_includes_excludes={
                OffenseType.PERSON: [
                    IncludesExcludesSet(
                        members=PersonOffenseIncludesExcludes,
                        excluded_set={
                            PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                        },
                    ),
                ],
                OffenseType.PROPERTY: [
                    IncludesExcludesSet(
                        members=PropertyOffenseIncludesExcludes,
                        excluded_set={
                            PropertyOffenseIncludesExcludes.ROBBERY,
                        },
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
                    IncludesExcludesSet(
                        members=DrugOffenseIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                OffenseType.PERSON: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was an offense against a person.",
                OffenseType.PROPERTY: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was a property offense.",
                OffenseType.PUBLIC_ORDER: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was a public order offense.",
                OffenseType.DRUG: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was a drug offense.",
                OffenseType.OTHER: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge was not a person, property, drug, or public order offense.",
                OffenseType.UNKNOWN: "A single day count of the number of people incarcerated pre-adjudication whose most serious charge type is not known.",
            },
        ),
        # TODO(#18071) Implement reused global includes/excludes
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=True,
            contexts=[Context(key=ContextKey.OTHER_RACE_DESCRIPTION, label="")],
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.HISPANIC_OR_LATINO: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                CensusRace.MORE_THAN_ONE_RACE: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race is listed as more than one race, such as White and Black.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race is listed as some other race, not included above.",
                CensusRace.UNKNOWN: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race is not known.",
                CensusRace.WHITE: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have not yet been adjudicated whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
            },
        ),
        # TODO(#18071) Implement reused global includes/excludes
        AggregatedDimension(
            dimension=BiologicalSex,
            required=True,
            dimension_to_description={
                BiologicalSex.MALE: "A single day count of the number of people incarcerated in the agency’s jurisdiction who have not been adjudicated whose biological sex is male.",
                BiologicalSex.FEMALE: "A single day count of the number of people incarcerated in the agency’s jurisdiction who have not been adjudicated whose biological sex is female.",
                BiologicalSex.UNKNOWN: "A single day count of the number of people incarcerated in the agency’s jurisdiction who have not been adjudicated whose biological sex is not known.",
            },
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
        ),
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        ),
    ],
)

post_adjudication_daily_population = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.POST_ADJUDICATION_POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Post-adjudication Daily Population",
    description="A single day count of the number of people incarcerated in the agency’s jurisdiction who have been adjudicated and convicted.",
    unit=MetricUnit.PEOPLE_INCARCERATED,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) implement reused includes/excludes
    includes_excludes=[
        IncludesExcludesSet(
            members=PostAdjudicationJailPopulation,
            excluded_set={
                PostAdjudicationJailPopulation.AWAITING_ARRAIGNMENT,
                PostAdjudicationJailPopulation.UNPAID_BAIL,
                PostAdjudicationJailPopulation.DENIAL_OF_BAIL,
                PostAdjudicationJailPopulation.REVOCATION_OF_BAIL,
                PostAdjudicationJailPopulation.PENDING_ASSESSMENT,
                PostAdjudicationJailPopulation.TRANSFERRED_TO_HOSPITAL,
                PostAdjudicationJailPopulation.PENDING_OUTCOME,
                PostAdjudicationJailPopulation.REVOCATION_PRETRIAL_RELEASE,
                PostAdjudicationJailPopulation.PRETRIAL_SUPERVISION_SANCTION,
                PostAdjudicationJailPopulation.US_MARSHALS_SERVICE,
                PostAdjudicationJailPopulation.TRIBAL_NATION,
                PostAdjudicationJailPopulation.FAILURE_TO_APPEAR,
                PostAdjudicationJailPopulation.FAILURE_TO_PAY,
                PostAdjudicationJailPopulation.HELD_FOR_OTHER_STATE,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_includes_excludes={
                OffenseType.PERSON: [
                    IncludesExcludesSet(
                        members=PersonOffenseIncludesExcludes,
                        excluded_set={
                            PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                        },
                    ),
                ],
                OffenseType.PROPERTY: [
                    IncludesExcludesSet(
                        members=PropertyOffenseIncludesExcludes,
                        excluded_set={
                            PropertyOffenseIncludesExcludes.ROBBERY,
                        },
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
                    IncludesExcludesSet(
                        members=DrugOffenseIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                OffenseType.PERSON: "A single day count of the number of people incarcerated post-adjudication whose most serious offense was a crime against a person.",
                OffenseType.PROPERTY: "A single day count of the number of people incarcerated post-adjudication whose most serious offense was a property offense.",
                OffenseType.PUBLIC_ORDER: "A single day count of the number of people incarcerated post-adjudication whose most serious offense was a public order offense.",
                OffenseType.DRUG: "A single day count of the number of people incarcerated post-adjudication whose most serious offense was a drug offense.",
                OffenseType.OTHER: "A single day count of the number of people incarcerated post-adjudication whose most serious offense was for another type of offense that was not a person, property, drug, or public order offense.",
                OffenseType.UNKNOWN: "A single day count of the number of people incarcerated post-adjudication whose most serious offense was not known.",
            },
        ),
        # TODO(#18071) Implement reused global includes/excludes
        AggregatedDimension(
            dimension=RaceAndEthnicity,
            required=True,
            contexts=[Context(key=ContextKey.OTHER_RACE_DESCRIPTION, label="")],
            dimension_to_description={
                CensusRace.AMERICAN_INDIAN_ALASKAN_NATIVE: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race is listed as Native American, American Indian, Native Alaskan, or similar. This includes people with origins in the original populations or Tribal groups of North, Central, or South America.",
                CensusRace.ASIAN: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race is listed as Asian. This includes people with origins in China, Japan, Korea, Laos, Vietnam, as well as India, Malaysia, the Philippines, and other countries in East and South Asia.",
                CensusRace.BLACK: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race is listed as Black or African-American. This includes people with origins in Kenya, Nigeria, Ghana, Ethiopia, or other countries in Sub-Saharan Africa.",
                CensusRace.HISPANIC_OR_LATINO: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race and ethnicity are listed as Hispanic or Latino. This includes people with origins in Mexico, Cuba, Puerto Rico, the Dominican Republic, and other Spanish-speaking countries in Central or South America, as well as people with origins in Brazil or other non-Spanish-speaking countries in Central or South America.",
                CensusRace.MORE_THAN_ONE_RACE: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race is listed as more than one race, such as White and Black.",
                CensusRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race is listed as Native Hawaiian, Pacific Islander, or similar. This includes people with origins in the original populations of Pacific islands such as Hawaii, Samoa, Fiji, Tahiti, or Papua New Guinea.",
                CensusRace.OTHER: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race is listed as some other race, not included above.",
                CensusRace.UNKNOWN: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race is not known.",
                CensusRace.WHITE: "A single-day count of the number of people incarcerated in the agency's jurisdiction who have been adjudicated and convicted whose race is listed as White, Caucasian, or Anglo. This includes people with origins in France, Italy, or other countries in Europe, as well as Israel, Palestine, Egypt, or other countries in the Middle East and North Africa.",
            },
        ),
        # TODO(#18071) Implement reused global includes/excludes
        AggregatedDimension(
            dimension=BiologicalSex,
            required=True,
            dimension_to_description={
                BiologicalSex.MALE: "A single day count of the number of people incarcerated in the agency’s jurisdiction who have been adjudicated whose biological sex is male.",
                BiologicalSex.FEMALE: "A single day count of the number of people incarcerated in the agency’s jurisdiction who have been adjudicated whose biological sex is female.",
                BiologicalSex.UNKNOWN: "A single day count of the number of people incarcerated in the agency’s jurisdiction who have been adjudicated whose biological sex is not known.",
            },
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
        ),
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        ),
    ],
)

total_releases = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.TOTAL_RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Total Releases",
    description="The number of total release events from the agency's jurisdiction after a period of incarceration.",
    additional_description="Releases are based on the number of events in which a person was released from the jurisdiction of the agency, not the number of individual people released. If the same person was released from jail three times in a time period, it would count as three releases.",
    unit=MetricUnit.RELEASES,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=TotalReleasesIncludesExcludes,
            excluded_set={TotalReleasesIncludesExcludes.TEMPORARY_RELEASES_EXCLUDE},
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        )
    ],
)

pre_adjudication_releases = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.PRE_ADJUDICATION_RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Pre-adjudication Releases",
    description="The number of release events from the agency’s jurisdiction after a period of pre-adjudication incarceration.",
    additional_description="Releases are based on the number of events in which a person was released from the jurisdiction of the agency, not the number of individual people released. If the same person was released from jail three times in a time period, it would count as three releases.",
    unit=MetricUnit.RELEASES,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=PreAdjudicationReleasesIncludesExcludes,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PreAdjudicationReleaseType,
            required=False,
            dimension_to_includes_excludes={
                PreAdjudicationReleaseType.AWAITING_TRIAL: [
                    IncludesExcludesSet(
                        members=PreAdjudicationReleasesOwnRecognizanceAwaitingTrialIncludesExcludes,
                    ),
                ],
                PreAdjudicationReleaseType.BAIL: [
                    IncludesExcludesSet(
                        members=PreAdjudicationReleasesMonetaryBailIncludesExcludes,
                        excluded_set={
                            PreAdjudicationReleasesMonetaryBailIncludesExcludes.BEFORE_HEARING,
                        },
                    ),
                ],
                PreAdjudicationReleaseType.DEATH: [
                    IncludesExcludesSet(
                        members=PreAdjudicationReleasesDeathIncludesExcludes,
                    ),
                ],
                PreAdjudicationReleaseType.AWOL: [
                    IncludesExcludesSet(
                        members=PreAdjudicationReleasesEscapeOrAWOLIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                PreAdjudicationReleaseType.AWAITING_TRIAL: "The number of pre-adjudication release events of people to their own recognizance while awaiting trial, without out any other form of supervision.",
                PreAdjudicationReleaseType.BAIL: "The number of pre-adjudication release events of people to bond while awaiting trial, without out any other form of supervision.",
                PreAdjudicationReleaseType.DEATH: "The number of pre-adjudication release events due to death of people in custody.",
                PreAdjudicationReleaseType.AWOL: "The number of pre-adjudication release events due to escape from custody or assessment as AWOL for more than 30 days.",
                PreAdjudicationReleaseType.OTHER: "The number of pre-adjudication release events from the agency’s jurisdiction that are not releases to pretrial supervision, to own recognizance awaiting trial, to monetary bail, death, or escape/AWOL status.",
                PreAdjudicationReleaseType.UNKNOWN: "The number of pre-adjudication release events from the agency’s jurisdiction whose release type is not known.",
            },
        ),
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        ),
    ],
)

post_adjudication_releases = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.POST_ADJUDICATION_RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Post-adjudication Releases",
    description="The number of release events from the agency’s jurisdiction following a sentence of a period of incarceration in jail due to a conviction for a criminal offense.",
    additional_description="Releases are based on the number of events in which a person was released from the jurisdiction of the agency, not the number of individual people released. If the same person was released from jail three times in a time period, it would count as three releases.",
    unit=MetricUnit.RELEASES,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=PostAdjudicationReleasesIncludesExcludes,
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PostAdjudicationReleaseType,
            required=False,
            # TODO(#18071) implement reused includes/excludes
            dimension_to_includes_excludes={
                PostAdjudicationReleaseType.PROBATION: [
                    IncludesExcludesSet(
                        members=PostAdjudicationReleasesProbationSupervisionIncludesExcludes,
                    ),
                ],
                PostAdjudicationReleaseType.PAROLE: [
                    IncludesExcludesSet(
                        members=PostAdjudicationReleasesParoleSupervisionIncludesExcludes,
                    ),
                ],
                PostAdjudicationReleaseType.COMMUNITY_SUPERVISION: [
                    IncludesExcludesSet(
                        members=PostAdjudicationReleasesOtherCommunitySupervisionIncludesExcludes,
                    ),
                ],
                PostAdjudicationReleaseType.NO_ADDITIONAL_CONTROL: [
                    IncludesExcludesSet(
                        members=PostAdjudicationReleasesNoAdditionalCorrectionalControlIncludesExcludes,
                    ),
                ],
                PostAdjudicationReleaseType.DEATH: [
                    IncludesExcludesSet(
                        members=PostAdjudicationReleasesDueToDeathIncludesExcludes,
                    ),
                ],
                PostAdjudicationReleaseType.AWOL: [
                    IncludesExcludesSet(
                        members=PostAdjudicationReleasesDueToEscapeOrAWOLIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                PostAdjudicationReleaseType.PROBATION: "The number of post-adjudication release events from the agency’s jurisdiction to probation supervision.",
                PostAdjudicationReleaseType.PAROLE: "The number of post-adjudication release events from the agency’s jurisdiction to parole supervision.",
                PostAdjudicationReleaseType.COMMUNITY_SUPERVISION: "The number of post-adjudication release events from the agency’s jurisdiction to another form of community supervision that is not probation or parole.",
                PostAdjudicationReleaseType.NO_ADDITIONAL_CONTROL: "The number of post-adjudication release events from the agency’s jurisdiction with no additional correctional control.",
                PostAdjudicationReleaseType.DEATH: "The number of post-adjudication release events from the agency’s jurisdiction due to death of people in custody.",
                PostAdjudicationReleaseType.AWOL: "The number of post-adjudication release events due to escape from custody or assessment as AWOL for more than 30 days.",
                PostAdjudicationReleaseType.OTHER: "The number of post-adjudication release events from the agency’s jurisdiction that are not releases to probation supervision, to parole supervision, to other community supervision, to no additional correctional control, due to death, or due to escape or AWOL status.",
                PostAdjudicationReleaseType.UNKNOWN: "The number of post-adjudication release events from the agency’s jurisdiction where the release type is not known.",
            },
        ),
        AggregatedDimension(
            dimension=BehavioralHealthNeedType,
            required=False,
            dimension_to_includes_excludes={
                BehavioralHealthNeedType.MENTAL_HEALTH: [
                    IncludesExcludesSet(
                        members=MentalHealthNeedIncludesExcludes,
                        excluded_set={
                            MentalHealthNeedIncludesExcludes.SELF_REPORT,
                            MentalHealthNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            MentalHealthNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            MentalHealthNeedIncludesExcludes.PSYCHOTROPIC_MEDICATION,
                            MentalHealthNeedIncludesExcludes.JAIL_CLASSIFICATION,
                            MentalHealthNeedIncludesExcludes.SUICIDE_RISK_SCREENING,
                        },
                    ),
                ],
                BehavioralHealthNeedType.SUBSTANCE_USE: [
                    IncludesExcludesSet(
                        members=SubstanceUseNeedIncludesExcludes,
                        excluded_set={
                            SubstanceUseNeedIncludesExcludes.SELF_REPORT,
                            SubstanceUseNeedIncludesExcludes.POSITIVE_DRUG_TEST,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_PRIOR,
                            SubstanceUseNeedIncludesExcludes.PRIOR_TREATMENT_HISTORY,
                            SubstanceUseNeedIncludesExcludes.MAT_RECEIVING,
                            SubstanceUseNeedIncludesExcludes.CLINICAL_ASSESSMENT_JAIL,
                        },
                    ),
                ],
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: [
                    IncludesExcludesSet(
                        members=CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes,
                        excluded_set={
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_MENTAL_HEALTH,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.ONLY_SUBSTANCE_USE,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.SUBSTANCE_USE_AND_OTHER_NEED,
                            CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes.MENTAL_HEALTH_AND_OTHER_NEED,
                        },
                    ),
                ],
                BehavioralHealthNeedType.OTHER: [
                    IncludesExcludesSet(
                        members=OtherBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            OtherBehavioralHealthNeedIncludesExcludes.CHRONIC_DISEASES,
                            OtherBehavioralHealthNeedIncludesExcludes.MENTAL_HEALTH_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.SUBSTANCE_USE_NEED,
                            OtherBehavioralHealthNeedIncludesExcludes.PSYCHOSOCIAL_PROBLEMS,
                            OtherBehavioralHealthNeedIncludesExcludes.ECONOMIC_HOUSING_EDUCATION,
                            OtherBehavioralHealthNeedIncludesExcludes.SOCIAL_SUPPORT_PROBLEMS,
                        },
                    ),
                ],
                BehavioralHealthNeedType.UNKNOWN: [
                    IncludesExcludesSet(
                        members=UnknownBehavioralHealthNeedIncludesExcludes,
                    ),
                ],
                BehavioralHealthNeedType.NONE: [
                    IncludesExcludesSet(
                        members=NoBehavioralHealthNeedIncludesExcludes,
                        excluded_set={
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_MENTAL_HEALTH_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.IDENTIFIED_SUBSTANCE_USE_NEED,
                            NoBehavioralHealthNeedIncludesExcludes.CO_OCCURRING_NEEDS,
                        },
                    ),
                ],
            },
            dimension_to_description={
                BehavioralHealthNeedType.MENTAL_HEALTH: "A condition involving significant changes in thinking, emotion, and/or behavior that cause distress and/or problems functioning in daily activities.",
                BehavioralHealthNeedType.SUBSTANCE_USE: "A condition involving a pattern of substance use that causes significant impairment or distress.",
                BehavioralHealthNeedType.CO_OCCURRING_DISORDERS: "The co-occurrence of both a mental health and a substance use need.",
                BehavioralHealthNeedType.OTHER: "Conditions associated with life stressors, crisis, and stress-related physical symptoms that cannot be classified as either a mental health or substance use condition.",
                BehavioralHealthNeedType.UNKNOWN: "Behavioral health needs are unknown.",
                BehavioralHealthNeedType.NONE: "The absence of an identified behavioral health condition.",
            },
        ),
    ],
)

staff_use_of_force_incidents = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Use of Force Incidents",
    description="The number of incidents in which agency staff use physical force to gain compliance from or control of a person who is under the agency’s jurisdiction.",
    additional_description="Incidents represent unique events where force was used, not the number of people or staff involved in those events.",
    unit=MetricUnit.INCIDENTS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=UseOfForceIncidentsIncludesExcludes,
            excluded_set={
                UseOfForceIncidentsIncludesExcludes.ROUTINE,
            },
        ),
    ],
)

grievances_upheld = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.GRIEVANCES_UPHELD,
    category=MetricCategory.FAIRNESS,
    display_name="Grievances Upheld",
    description="The number of complaints from people in jail in the agency’s jurisdiction that were received through the official grievance process and upheld or substantiated.",
    additional_description="Count grievances in the time period in which they were resolved, not when they were received or occurred. For instance, if a complaint was received on November 8, 2021, and resolved on January 14, 2022, that grievance would be counted in 2022.",
    unit=MetricUnit.GRIEVANCES_UPHELD,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=GrievancesUpheldIncludesExcludes,
            excluded_set={
                GrievancesUpheldIncludesExcludes.UNSUBSTANTIATED,
                GrievancesUpheldIncludesExcludes.PENDING_RESOLUTION,
                GrievancesUpheldIncludesExcludes.INFORMAL,
                GrievancesUpheldIncludesExcludes.DUPLICATE,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=GrievancesUpheldType,
            required=False,
            dimension_to_includes_excludes={
                GrievancesUpheldType.LIVING_CONDITIONS: [
                    IncludesExcludesSet(
                        members=LivingConditionsIncludesExcludes,
                    ),
                ],
                GrievancesUpheldType.PERSONAL_SAFETY: [
                    IncludesExcludesSet(
                        members=PersonalSafetyIncludesExcludes,
                    ),
                ],
                GrievancesUpheldType.DISCRIMINATION: [
                    IncludesExcludesSet(
                        members=DiscriminationRacialBiasReligiousIncludesExcludes,
                    ),
                ],
                GrievancesUpheldType.ACCESS_TO_HEALTH_CARE: [
                    IncludesExcludesSet(
                        members=AccessToHealthCareIncludesExcludes,
                    ),
                ],
                GrievancesUpheldType.LEGAL: [
                    IncludesExcludesSet(
                        members=LegalIncludesExcludes,
                    ),
                ],
            },
            dimension_to_description={
                GrievancesUpheldType.LIVING_CONDITIONS: "The number of grievances upheld that relate to the living conditions of people who are incarcerated under the agency’s jurisdiction.",
                GrievancesUpheldType.PERSONAL_SAFETY: "The number of grievances upheld that relate to the personal safety of people who are incarcerated.",
                GrievancesUpheldType.DISCRIMINATION: "The number of grievances upheld that relate to an act of discrimination toward, racial bias against, or interference of religious practices of people who are incarcerated.",
                GrievancesUpheldType.ACCESS_TO_HEALTH_CARE: "The number of grievances upheld that relate to the accessibility of health care to people who are incarcerated.",
                GrievancesUpheldType.LEGAL: "The number of grievances upheld that relate to the person under the agency’s jurisdiction having access to the legal process.",
                GrievancesUpheldType.OTHER: "The number of grievances upheld that relate to another issue or concern that is not related to living conditions, personal safety, discrimination or racial bias, access to health care, or legal concerns.",
                GrievancesUpheldType.UNKNOWN: "The number of grievances upheld that relate to an issue or concern that is not known.",
            },
        )
    ],
)
