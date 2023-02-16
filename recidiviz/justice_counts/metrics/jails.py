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
from recidiviz.justice_counts.dimensions.jails import (
    ExpenseType,
    FundingType,
    GrievancesUpheldType,
    ReleaseType,
    StaffType,
)
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    GenderRestricted,
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
    DiscriminationRacialBiasReligiousIncludesExcludes,
    ExpensesIncludesExcludes,
    FacilitiesAndEquipmentIncludesExcludes,
    FundingIncludesExcludes,
    GrievancesUpheldIncludesExcludes,
    HealthCareForPeopleWhoAreIncarceratedIncludesExcludes,
    LegalIncludesExcludes,
    LivingConditionsIncludesExcludes,
    ManagementAndOperationsStaffIncludesExcludes,
    PersonalSafetyIncludesExcludes,
    PersonnelIncludesExcludes,
    PreAdjudicationAdmissionsIncludesExcludes,
    PreAdjudicationReleasesDeathIncludesExcludes,
    PreAdjudicationReleasesEscapeOrAWOLIncludesExcludes,
    PreAdjudicationReleasesIncludesExcludes,
    PreAdjudicationReleasesMonetaryBailIncludesExcludes,
    PreAdjudicationReleasesOwnRecognizanceAwaitingTrialIncludesExcludes,
    ProgrammaticStaffIncludesExcludes,
    SecurityStaffIncludesExcludes,
    StaffIncludesExcludes,
    TrainingIncludesExcludes,
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
    IncludesExcludesSet,
    MetricCategory,
    MetricDefinition,
)
from recidiviz.persistence.database.schema.justice_counts.schema import (
    MeasurementType,
    MetricType,
    ReportingFrequency,
    System,
)

residents = MetricDefinition(
    system=System.JAILS,
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
    system=System.JAILS,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    # TODO(#17577) implement multiple includes/excludes tables
    includes_excludes=IncludesExcludesSet(
        members=FundingIncludesExcludes,
        excluded_set={
            FundingIncludesExcludes.OPERATIONS_MAINTENANCE,
            FundingIncludesExcludes.JUVENILE,
            FundingIncludesExcludes.NON_JAIL_ACTIVITIES,
            FundingIncludesExcludes.LAW_ENFORCEMENT,
        },
    ),
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
                FundingType.STATE_APPROPRIATION: IncludesExcludesSet(
                    members=StateAppropriationIncludesExcludes,
                    excluded_set={
                        StateAppropriationIncludesExcludes.PROPOSED,
                        StateAppropriationIncludesExcludes.PRELIMINARY,
                        StateAppropriationIncludesExcludes.GRANTS,
                    },
                ),
                FundingType.COUNTY_MUNICIPAL: IncludesExcludesSet(
                    members=CountyOrMunicipalAppropriationIncludesExcludes,
                    excluded_set={
                        CountyOrMunicipalAppropriationIncludesExcludes.PROPOSED,
                        CountyOrMunicipalAppropriationIncludesExcludes.PRELIMINARY,
                    },
                ),
                FundingType.GRANTS: IncludesExcludesSet(
                    members=GrantsIncludesExcludes,
                ),
                FundingType.COMMISSARY_FEES: IncludesExcludesSet(
                    members=CommissaryAndFeesIncludesExcludes,
                ),
                FundingType.CONTRACT_BEDS: IncludesExcludesSet(
                    members=ContractBedsFundingIncludesExcludes,
                ),
            },
        )
    ],
)

expenses = MetricDefinition(
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    system=System.JAILS,
    description="The amount the agency spent for the operation and maintenance of jail facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    metric_type=MetricType.EXPENSES,
    display_name="Expenses",
    measurement_type=MeasurementType.DELTA,
    category=MetricCategory.CAPACITY_AND_COST,
    # TODO(#17577) Implement multiple includes/excludes tables
    includes_excludes=IncludesExcludesSet(
        members=ExpensesIncludesExcludes,
        excluded_set={
            ExpensesIncludesExcludes.OPERATIONS_MAINTENANCE,
            ExpensesIncludesExcludes.JUVENILE,
            ExpensesIncludesExcludes.NON_JAIL_ACTIVITIES,
            ExpensesIncludesExcludes.LAW_ENFORCEMENT,
        },
    ),
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
                ExpenseType.PERSONNEL: IncludesExcludesSet(
                    members=PersonnelIncludesExcludes,
                    excluded_set={
                        PersonnelIncludesExcludes.COMPANIES_CONTRACTED,
                    },
                ),
                ExpenseType.TRAINING: IncludesExcludesSet(
                    members=TrainingIncludesExcludes,
                    excluded_set={
                        TrainingIncludesExcludes.NO_COST_PROGRAMS,
                    },
                ),
                ExpenseType.FACILITIES: IncludesExcludesSet(
                    members=FacilitiesAndEquipmentIncludesExcludes,
                ),
                ExpenseType.HEALTH_CARE: IncludesExcludesSet(
                    members=HealthCareForPeopleWhoAreIncarceratedIncludesExcludes,
                    excluded_set={
                        HealthCareForPeopleWhoAreIncarceratedIncludesExcludes.TRANSPORT_COSTS,
                    },
                ),
                ExpenseType.CONTRACT_BEDS: IncludesExcludesSet(
                    members=ContractBedsExpensesIncludesExcludes,
                ),
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
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        members=StaffIncludesExcludes,
        excluded_set={
            StaffIncludesExcludes.VOLUNTEER,
            StaffIncludesExcludes.INTERN,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_includes_excludes={
                StaffType.SECURITY: IncludesExcludesSet(
                    members=SecurityStaffIncludesExcludes,
                    excluded_set={
                        SecurityStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.MANAGEMENT_AND_OPERATIONS: IncludesExcludesSet(
                    members=ManagementAndOperationsStaffIncludesExcludes,
                    excluded_set={
                        ManagementAndOperationsStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.CLINICAL_AND_MEDICAL: IncludesExcludesSet(
                    members=ClinicalAndMedicalStaffIncludesExcludes,
                    excluded_set={
                        ClinicalAndMedicalStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.PROGRAMMATIC: IncludesExcludesSet(
                    members=ProgrammaticStaffIncludesExcludes,
                    excluded_set={
                        ProgrammaticStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.VACANT: IncludesExcludesSet(
                    members=VacantPositionsIncludesExcludes,
                    excluded_set={
                        VacantPositionsIncludesExcludes.FILLED,
                    },
                ),
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
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Readmissions",
    description="The number of admission events to the agency’s jurisdiction of people who were incarcerated in the agency’s jurisdiction within the previous year (365 days).",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    # TODO(#18071) implement reused includes/excludes
)

pre_adjudication_admissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.PRE_ADJUDICATION_ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Pre-adjudication Admissions",
    description="The number of admission events to the agency’s jurisdiction in which the person has not yet been adjudicated.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) implement reused includes/excludes
    includes_excludes=IncludesExcludesSet(
        members=PreAdjudicationAdmissionsIncludesExcludes,
        excluded_set={
            PreAdjudicationAdmissionsIncludesExcludes.TEMPORARY_ABSENCE,
            PreAdjudicationAdmissionsIncludesExcludes.MOVING,
        },
    ),
    # TODO(#18071) implement reused includes/excludes
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_includes_excludes={
                OffenseType.PERSON: IncludesExcludesSet(
                    members=PersonOffenseIncludesExcludes,
                    excluded_set={
                        PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                    },
                ),
                OffenseType.PROPERTY: IncludesExcludesSet(
                    members=PropertyOffenseIncludesExcludes,
                    excluded_set={
                        PropertyOffenseIncludesExcludes.ROBBERY,
                    },
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
                    members=DrugOffenseIncludesExcludes,
                ),
            },
            dimension_to_description={
                OffenseType.PERSON: "The number of pre-adjudication admission events in which the most serious charge was for an offense against a person.",
                OffenseType.PROPERTY: "The number of pre-adjudication admission events in which the most serious charge was for a property offense.",
                OffenseType.PUBLIC_ORDER: "The number of pre-adjudication admission events in which the most serious charge was for a public order offense.",
                OffenseType.DRUG: "The number of pre-adjudication admission events in which the most serious charge was for a drug offense.",
                OffenseType.OTHER: "The number of pre-adjudication admission events in which the most serious charge was for another type of offense that was not a person, property, public order, or drug offense.",
                OffenseType.UNKNOWN: "The number of pre-adjudication admission events in which the most serious offense charge type is not known.",
            },
        )
    ],
)

post_adjudication_admissions = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.POST_ADJUDICATION_ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Post-adjudication Admissions",
    description="The number of admission events to the agency’s jurisdiction in which the person has been adjudicated.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) implement reused includes/excludes
    includes_excludes=IncludesExcludesSet(
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
    # TODO(#18071) implement reused includes/excludes
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_includes_excludes={
                OffenseType.PERSON: IncludesExcludesSet(
                    members=PersonOffenseIncludesExcludes,
                    excluded_set={
                        PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                    },
                ),
                OffenseType.PROPERTY: IncludesExcludesSet(
                    members=PropertyOffenseIncludesExcludes,
                    excluded_set={
                        PropertyOffenseIncludesExcludes.ROBBERY,
                    },
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
                    members=DrugOffenseIncludesExcludes,
                ),
            },
            dimension_to_description={
                OffenseType.PERSON: "The number of post-adjudication admission events in which the most serious offense was a crime against a person.",
                OffenseType.PROPERTY: "The number of post-adjudication admission events in which the most serious offense was a property crime.",
                OffenseType.PUBLIC_ORDER: "The number of post-adjudication admission events in which the most serious offense was a public order offense.",
                OffenseType.DRUG: "The number of post-adjudication admission events in which the most serious offense was a drug offense.",
                OffenseType.OTHER: "The number of post-adjudication admission events in which the most serious offense was for another type of offense that was not a person, property, drug, or public order offense.",
                OffenseType.UNKNOWN: "The number of post-adjudication admission events in which the most serious offense charge type is not known.",
            },
        )
    ],
)

pre_adjudication_daily_population = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.PRE_ADJUDICATION_POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Pre-adjudication Daily Population",
    description="A single day count of the number of people incarcerated in the agency’s jurisdiction who have not yet been adjudicated.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) Implement reused global includes/excludes
    includes_excludes=IncludesExcludesSet(
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
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_includes_excludes={
                OffenseType.PERSON: IncludesExcludesSet(
                    members=PersonOffenseIncludesExcludes,
                    excluded_set={
                        PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                    },
                ),
                OffenseType.PROPERTY: IncludesExcludesSet(
                    members=PropertyOffenseIncludesExcludes,
                    excluded_set={
                        PropertyOffenseIncludesExcludes.ROBBERY,
                    },
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
                    members=DrugOffenseIncludesExcludes,
                ),
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
        # TODO(#17579) Implement Y/N Tables
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        # TODO(#18071) Implement reused global includes/excludes
        AggregatedDimension(
            dimension=BiologicalSex,
            required=True,
            dimension_to_description={
                BiologicalSex.MALE: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is male.",
                BiologicalSex.FEMALE: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is female.",
                BiologicalSex.UNKNOWN: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is not known.",
            },
            dimension_to_includes_excludes={
                BiologicalSex.MALE: IncludesExcludesSet(
                    members=MaleBiologicalSexIncludesExcludes,
                    excluded_set={MaleBiologicalSexIncludesExcludes.UNKNOWN},
                ),
                BiologicalSex.FEMALE: IncludesExcludesSet(
                    members=FemaleBiologicalSexIncludesExcludes,
                    excluded_set={FemaleBiologicalSexIncludesExcludes.UNKNOWN},
                ),
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
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#18071) implement reused includes/excludes
    includes_excludes=IncludesExcludesSet(
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
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_includes_excludes={
                OffenseType.PERSON: IncludesExcludesSet(
                    members=PersonOffenseIncludesExcludes,
                    excluded_set={
                        PersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                    },
                ),
                OffenseType.PROPERTY: IncludesExcludesSet(
                    members=PropertyOffenseIncludesExcludes,
                    excluded_set={
                        PropertyOffenseIncludesExcludes.ROBBERY,
                    },
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
                    members=DrugOffenseIncludesExcludes,
                ),
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
        # TODO(#17579) Implement Y/N Tables
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        # TODO(#18071) Implement reused global includes/excludes
        AggregatedDimension(
            dimension=BiologicalSex,
            required=True,
            dimension_to_description={
                BiologicalSex.MALE: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is male.",
                BiologicalSex.FEMALE: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is female.",
                BiologicalSex.UNKNOWN: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is not known.",
            },
            dimension_to_includes_excludes={
                BiologicalSex.MALE: IncludesExcludesSet(
                    members=MaleBiologicalSexIncludesExcludes,
                    excluded_set={MaleBiologicalSexIncludesExcludes.UNKNOWN},
                ),
                BiologicalSex.FEMALE: IncludesExcludesSet(
                    members=FemaleBiologicalSexIncludesExcludes,
                    excluded_set={FemaleBiologicalSexIncludesExcludes.UNKNOWN},
                ),
            },
        ),
    ],
)

pre_adjudication_releases = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.PRE_ADJUDICATION_RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Pre-adjudication Releases",
    description="The number of release events from the agency’s jurisdiction after a period of pre-adjudication incarceration.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=PreAdjudicationReleasesIncludesExcludes,
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ReleaseType,
            required=False,
            dimension_to_includes_excludes={
                ReleaseType.AWAITING_TRIAL: IncludesExcludesSet(
                    members=PreAdjudicationReleasesOwnRecognizanceAwaitingTrialIncludesExcludes,
                ),
                ReleaseType.BAIL: IncludesExcludesSet(
                    members=PreAdjudicationReleasesMonetaryBailIncludesExcludes,
                    excluded_set={
                        PreAdjudicationReleasesMonetaryBailIncludesExcludes.BEFORE_HEARING,
                    },
                ),
                ReleaseType.DEATH: IncludesExcludesSet(
                    members=PreAdjudicationReleasesDeathIncludesExcludes,
                ),
                ReleaseType.AWOL: IncludesExcludesSet(
                    members=PreAdjudicationReleasesEscapeOrAWOLIncludesExcludes,
                ),
            },
            dimension_to_description={
                ReleaseType.AWAITING_TRIAL: "The number of pre-adjudication release events of people to their own recognizance while awaiting trial, without out any other form of supervision.",
                ReleaseType.BAIL: "The number of pre-adjudication release events of people to bond while awaiting trial, without out any other form of supervision.",
                ReleaseType.DEATH: "The number of pre-adjudication release events due to death of people in custody.",
                ReleaseType.AWOL: "The number of pre-adjudication release events due to escape from custody or assessment as AWOL for more than 30 days.",
                ReleaseType.OTHER: "The number of pre-adjudication release events from the agency’s jurisdiction that are not releases to pretrial supervision, to own recognizance awaiting trial, to monetary bail, death, or escape/AWOL status.",
                ReleaseType.UNKNOWN: "The number of pre-adjudication release events from the agency’s jurisdiction whose release type is not known.",
            },
        )
    ],
)

staff_use_of_force_incidents = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Use of Force Incidents",
    description="The number of incidents in which agency staff use physical force to gain compliance from or control of a person who is under the agency’s jurisdiction.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        members=UseOfForceIncidentsIncludesExcludes,
        excluded_set={
            UseOfForceIncidentsIncludesExcludes.ROUTINE,
        },
    ),
)

grievances_upheld = MetricDefinition(
    system=System.JAILS,
    metric_type=MetricType.GRIEVANCES_UPHELD,
    category=MetricCategory.FAIRNESS,
    display_name="Grievances Upheld",
    description="The number of complaints from people in jail in the agency’s jurisdiction that were received through the official grievance process and upheld or substantiated.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        members=GrievancesUpheldIncludesExcludes,
        excluded_set={
            GrievancesUpheldIncludesExcludes.UNSUBSTANTIATED,
            GrievancesUpheldIncludesExcludes.PENDING_RESOLUTION,
            GrievancesUpheldIncludesExcludes.INFORMAL,
            GrievancesUpheldIncludesExcludes.DUPLICATE,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=GrievancesUpheldType,
            required=False,
            dimension_to_includes_excludes={
                GrievancesUpheldType.LIVING_CONDITIONS: IncludesExcludesSet(
                    members=LivingConditionsIncludesExcludes,
                ),
                GrievancesUpheldType.PERSONAL_SAFETY: IncludesExcludesSet(
                    members=PersonalSafetyIncludesExcludes,
                ),
                GrievancesUpheldType.DISCRIMINATION: IncludesExcludesSet(
                    members=DiscriminationRacialBiasReligiousIncludesExcludes,
                ),
                GrievancesUpheldType.ACCESS_TO_HEALTH_CARE: IncludesExcludesSet(
                    members=AccessToHealthCareIncludesExcludes,
                ),
                GrievancesUpheldType.LEGAL: IncludesExcludesSet(
                    members=LegalIncludesExcludes,
                ),
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
