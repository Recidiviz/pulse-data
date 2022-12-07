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

from recidiviz.justice_counts.dimensions.jails_and_prisons import (
    GrievancesUpheldType,
    PrisonsExpenseType,
    PrisonsFundingType,
    PrisonsOffenseType,
    PrisonsReadmissionType,
    PrisonsReleaseType,
    PrisonsStaffType,
)
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.includes_excludes.person import (
    FemaleBiologicalSexIncludesExcludes,
    MaleBiologicalSexIncludesExcludes,
)
from recidiviz.justice_counts.includes_excludes.prisons import (
    PopulationIncludesExcludes,
    PrisonAdmissionsIncludesExcludes,
    PrisonClinicalStaffIncludesExcludes,
    PrisonDrugOffenseIncludesExcludes,
    PrisonExpensesContractBedsIncludesExcludes,
    PrisonExpensesFacilitiesAndEquipmentIncludesExcludes,
    PrisonExpensesHealthCareIncludesExcludes,
    PrisonExpensesIncludesExcludes,
    PrisonExpensesPersonnelIncludesExcludes,
    PrisonExpensesTrainingIncludesExcludes,
    PrisonFundingIncludesExcludes,
    PrisonGrievancesDiscriminationIncludesExcludes,
    PrisonGrievancesHealthCareIncludesExcludes,
    PrisonGrievancesIncludesExcludes,
    PrisonGrievancesLegalIncludesExcludes,
    PrisonGrievancesLivingConditionsIncludesExcludes,
    PrisonGrievancesPersonalSafetyIncludesExcludes,
    PrisonManagementAndOperationsStaffIncludesExcludes,
    PrisonPersonOffenseIncludesExcludes,
    PrisonProgrammaticStaffIncludesExcludes,
    PrisonPropertyOffenseIncludesExcludes,
    PrisonPublicOrderOffenseIncludesExcludes,
    PrisonReadmissionsIncludesExcludes,
    PrisonReadmissionsNewConvictionIncludesExcludes,
    PrisonReadmissionsParoleIncludesExcludes,
    PrisonReadmissionsProbationIncludesExcludes,
    PrisonReleasesCommunitySupervisionIncludesExcludes,
    PrisonReleasesDeathIncludesExcludes,
    PrisonReleasesIncludesExcludes,
    PrisonReleasesNoControlIncludesExcludes,
    PrisonReleasesToParoleIncludesExcludes,
    PrisonReleasesToProbationIncludesExcludes,
    PrisonSecurityStaffIncludesExcludes,
    PrisonsFundingCommissaryAndFeesIncludesExcludes,
    PrisonsFundingContractBedsIncludesExcludes,
    PrisonsFundingGrantsIncludesExcludes,
    PrisonsFundingStateAppropriationIncludesExcludes,
    PrisonStaffIncludesExcludes,
    PrisonUseOfForceIncludesExcludes,
    VacantPrisonStaffIncludesExcludes,
)
from recidiviz.justice_counts.metrics.metric_definition import (
    AggregatedDimension,
    Definition,
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
    system=System.PRISONS,
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
    system=System.PRISONS,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    includes_excludes=IncludesExcludesSet(
        members=PrisonFundingIncludesExcludes,
        excluded_set={
            PrisonFundingIncludesExcludes.BIENNIUM_FUNDING,
            PrisonFundingIncludesExcludes.MULTI_YEAR_APPROPRIATIONS,
            PrisonFundingIncludesExcludes.JAIL_OPERATIONS,
            PrisonFundingIncludesExcludes.NON_PRISON_ACTIVITIES,
            PrisonFundingIncludesExcludes.JUVENILE_JAILS,
            PrisonFundingIncludesExcludes.LAW_ENFORCEMENT,
        },
    ),
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsFundingType,
            required=False,
            dimension_to_description={
                PrisonsFundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                PrisonsFundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                PrisonsFundingType.COMMISSARY_AND_FEES: "The amount of funding the agency collected through sales and/or fees charged to people who are incarcerated under the jurisdiction of the agency or their visitors.",
                PrisonsFundingType.CONTRACT_BEDS: "The amount of funding the agency collected through contracts to provide custody and care for people who are incarcerated under the jurisdiction of another agency.",
                PrisonsFundingType.OTHER: "The amount of funding for the operation and maintenance of prison facilities and the care of people who are incarcerated that is not appropriated by the state, funded through grants, earned from commissary and fees, or collected from contracted beds.",
                PrisonsFundingType.UNKNOWN: "The amount of funding for the operation and maintenance of prison facilities and the care of people who are incarcerated for which the source is not known.",
            },
            dimension_to_includes_excludes={
                PrisonsFundingType.STATE_APPROPRIATION: IncludesExcludesSet(
                    members=PrisonsFundingStateAppropriationIncludesExcludes,
                    excluded_set={
                        PrisonsFundingStateAppropriationIncludesExcludes.PROPOSED,
                        PrisonsFundingStateAppropriationIncludesExcludes.PRELIMINARY,
                        PrisonsFundingStateAppropriationIncludesExcludes.GRANTS,
                    },
                ),
                PrisonsFundingType.GRANTS: IncludesExcludesSet(
                    members=PrisonsFundingGrantsIncludesExcludes,
                ),
                PrisonsFundingType.COMMISSARY_AND_FEES: IncludesExcludesSet(
                    members=PrisonsFundingCommissaryAndFeesIncludesExcludes,
                ),
                PrisonsFundingType.CONTRACT_BEDS: IncludesExcludesSet(
                    members=PrisonsFundingContractBedsIncludesExcludes,
                ),
            },
        )
    ],
)

total_staff = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the agency for the operation and maintenance of the prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    definitions=[
        Definition(
            term="Full-time staff",
            definition="Number of people employed in a full-time (0.9+) capacity.",
        )
    ],
    includes_excludes=IncludesExcludesSet(
        members=PrisonStaffIncludesExcludes,
        excluded_set={
            PrisonStaffIncludesExcludes.VOLUNTEER,
            PrisonStaffIncludesExcludes.INTERN,
        },
    ),
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    specified_contexts=[],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsStaffType,
            required=False,
            dimension_to_description={
                PrisonsStaffType.SECURITY: "The number of full-time equivalent positions that work directly with people who are incarcerated and are responsible for their custody, supervision, and monitoring.",
                PrisonsStaffType.MANAGEMENT_AND_OPERATIONS: "The number of full-time equivalent positions that do not work directly with people who are incarcerated but support the day-to-day operations of the agency.",
                PrisonsStaffType.CLINICAL_OR_MEDICAL: "The number of full-time equivalent positions that work directly with people who are incarcerated and are responsible for their health.",
                PrisonsStaffType.PROGRAMMATIC: "The number of full-time equivalent positions that are not medical or clinical staff and provide services and programming to people who are incarcerated.",
                PrisonsStaffType.OTHER: "The number of full-time equivalent positions dedicated to the operation and maintenance of prison facilities under the jurisdiction of the agency that are not security staff, management and operations staff, clinical or medical staff, or programmatic staff.",
                PrisonsStaffType.UNKNOWN: "The number of full-time equivalent positions dedicated to the operation and maintenance of prison facilities under the jurisdiction of the agency that are of an unknown type.",
                PrisonsStaffType.VACANT: "The number of full-time equivalent positions dedicated to the operation and maintenance of jail facilities under the jurisdiction of the agency of any type that are budgeted but not currently filled.",
            },
            dimension_to_includes_excludes={
                PrisonsStaffType.SECURITY: IncludesExcludesSet(
                    members=PrisonSecurityStaffIncludesExcludes,
                    excluded_set={
                        PrisonSecurityStaffIncludesExcludes.VACANT,
                    },
                ),
                PrisonsStaffType.MANAGEMENT_AND_OPERATIONS: IncludesExcludesSet(
                    members=PrisonManagementAndOperationsStaffIncludesExcludes,
                    excluded_set={
                        PrisonManagementAndOperationsStaffIncludesExcludes.VACANT,
                    },
                ),
                PrisonsStaffType.CLINICAL_OR_MEDICAL: IncludesExcludesSet(
                    members=PrisonClinicalStaffIncludesExcludes,
                    excluded_set={
                        PrisonClinicalStaffIncludesExcludes.VACANT,
                    },
                ),
                PrisonsStaffType.PROGRAMMATIC: IncludesExcludesSet(
                    members=PrisonProgrammaticStaffIncludesExcludes,
                    excluded_set={
                        PrisonProgrammaticStaffIncludesExcludes.VACANT,
                        PrisonProgrammaticStaffIncludesExcludes.VOLUNTEER,
                    },
                ),
                PrisonsStaffType.VACANT: IncludesExcludesSet(
                    members=VacantPrisonStaffIncludesExcludes,
                    excluded_set={
                        VacantPrisonStaffIncludesExcludes.FILLED,
                    },
                ),
            },
        )
    ],
)

expenses = MetricDefinition(
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    system=System.PRISONS,
    description="The amount spent by the agency for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    metric_type=MetricType.EXPENSES,
    display_name="Expenses",
    specified_contexts=[],
    measurement_type=MeasurementType.DELTA,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    includes_excludes=IncludesExcludesSet(
        members=PrisonExpensesIncludesExcludes,
        excluded_set={
            PrisonExpensesIncludesExcludes.BIENNNIUM_EXPENSES,
            PrisonExpensesIncludesExcludes.MULTI_YEAR,
            PrisonExpensesIncludesExcludes.JAIL_FACILITY,
            PrisonExpensesIncludesExcludes.JUVENILE_JAIL,
            PrisonExpensesIncludesExcludes.LAW_ENFORCEMENT,
            PrisonExpensesIncludesExcludes.NON_PRISON_ACTIVITIES,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsExpenseType,
            required=False,
            dimension_to_description={
                PrisonsExpenseType.PERSONNEL: "The amount spent by the agency to employ personnel involved in the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                PrisonsExpenseType.TRAINING: "The amount spent by the agency on the training of personnel involved in the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                PrisonsExpenseType.FACILITIES_AND_EQUIPMENT: "The amount spent by the agency for the purchase and use of the physical plant and property owned and operated by the agency and equipment used to support maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                PrisonsExpenseType.HEALTH_CARE: "The amount spent by the agency on medical care for people who are incarcerated under the jurisdiction of the agency.",
                PrisonsExpenseType.CONTRACT_BEDS: "The amount spent by the agency on contracts with other agencies to provide custody and care for people who are incarcerated under the jurisdiction of the agency.",
                PrisonsExpenseType.OTHER: "The amount spent by the agency on other costs relating to the operation and maintenance of prison facilities and the care of people who are incarcerated that are not personnel, training, facilities and equipment, health care, or contract bed expenses.",
                PrisonsExpenseType.UNKNOWN: "The amount spent by the agency on costs relating to the operation and maintenance of prison facilities and the care of people who are incarcerated for a purpose that is not known.",
            },
            dimension_to_includes_excludes={
                PrisonsExpenseType.PERSONNEL: IncludesExcludesSet(
                    members=PrisonExpensesPersonnelIncludesExcludes,
                    excluded_set={PrisonExpensesPersonnelIncludesExcludes.CONTRACTORS},
                ),
                PrisonsExpenseType.TRAINING: IncludesExcludesSet(
                    members=PrisonExpensesTrainingIncludesExcludes,
                    excluded_set={PrisonExpensesTrainingIncludesExcludes.FREE_PROGRAMS},
                ),
                PrisonsExpenseType.FACILITIES_AND_EQUIPMENT: IncludesExcludesSet(
                    members=PrisonExpensesFacilitiesAndEquipmentIncludesExcludes,
                ),
                PrisonsExpenseType.HEALTH_CARE: IncludesExcludesSet(
                    members=PrisonExpensesHealthCareIncludesExcludes,
                    excluded_set={
                        PrisonExpensesHealthCareIncludesExcludes.TRANSPORTATION
                    },
                ),
                PrisonsExpenseType.CONTRACT_BEDS: IncludesExcludesSet(
                    members=PrisonExpensesContractBedsIncludesExcludes,
                ),
            },
        )
    ],
)

readmissions = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.READMISSIONS,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Readmissions",
    description="The number of admission events to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within the previous three years (1,096 days).",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="If a person admitted on June 23, 2022, had been incarcerated at any time between June 23, 2019, and June 23, 2022, it would count as a readmission. This metric is based on admission events, so if the same person is readmitted three times in a time period, it would count as three readmissions.",
    specified_contexts=[],
    includes_excludes=IncludesExcludesSet(
        members=PrisonReadmissionsIncludesExcludes,
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsReadmissionType,
            required=False,
            dimension_to_description={
                PrisonsReadmissionType.NEW_CONVICTION: "The number of incarceration admissions due to new criminal conviction to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                PrisonsReadmissionType.RETURN_FROM_PROBATION: "The number of admissions due to probation hold, sanction, or revocation to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                PrisonsReadmissionType.RETURN_FROM_PAROLE: "The number of incarceration admissions due to parole hold, sanction, or revocation to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                PrisonsReadmissionType.OTHER_READMISSIONS: "The number of admissions, which were not admissions for a new conviction, admissions for a return from probation, or admissions for a return from parole, but an other admission to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                PrisonsReadmissionType.UNKNOWN_READMISSIONS: "The number of admissions, for an unknown reason, to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
            },
            dimension_to_includes_excludes={
                PrisonsReadmissionType.NEW_CONVICTION: IncludesExcludesSet(
                    members=PrisonReadmissionsNewConvictionIncludesExcludes,
                ),
                PrisonsReadmissionType.RETURN_FROM_PROBATION: IncludesExcludesSet(
                    members=PrisonReadmissionsProbationIncludesExcludes,
                ),
                PrisonsReadmissionType.RETURN_FROM_PAROLE: IncludesExcludesSet(
                    members=PrisonReadmissionsParoleIncludesExcludes,
                ),
            },
        )
    ],
)

admissions = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.ADMISSIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Admissions",
    description="The number of admission events to agency’s prison jurisdiction.",
    measurement_type=MeasurementType.DELTA,
    reporting_note="Admissions are based on the number of events in which a person was incarcerated in a prison facility, not the number of individual people who entered the facility. If the same person was admitted to prison three times in a time period, it would count as three admissions.",
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=PrisonAdmissionsIncludesExcludes,
        excluded_set={
            PrisonAdmissionsIncludesExcludes.RETURNING_FROM_ABSENCE,
            PrisonAdmissionsIncludesExcludes.TRANSFERRED_BETWEEN_FACILITIES,
        },
    ),
    specified_contexts=[],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsOffenseType,
            required=False,
            display_name="Prison Offense Type",
            dimension_to_description={
                PrisonsOffenseType.PERSON: "The number of admission events to the agency’s prison jurisdiction for which the most serious offense was a crime against a person (the definition of person offenses configured in Section 2.2 will be applied to this section).",
                PrisonsOffenseType.PROPERTY: "The number of admission events to the agency’s prison jurisdiction for which the most serious offense was a property offense (the definition of property offenses configured in Section 2.3 will be applied to this section).",
                PrisonsOffenseType.PUBLIC_ORDER: "The number of post-adjudication admissions to the agency’s jail jurisdiction for which the most serious offense was a public order offense (the definition of drug offenses configured in Section 2.4 will be applied to this section).",
                PrisonsOffenseType.DRUG: "The number of admissions to the agency’s prison jurisdiction for which the most serious offense was a drug offense (the definition of public order offenses configured in Section 2.5 will be applied to this section).",
                PrisonsOffenseType.OTHER: "The number of admissions to the agency’s prison jurisdiction for which the most serious offense was for another type of offense that was not a person offense, a property offense, a drug offense, or a public order offense (the definition of other offenses configured in Section 2.6 will be applied to this section).",
                PrisonsOffenseType.UNKNOWN: "The number of admissions to the agency’s prison jurisdiction for which the most serious offense charge type is not known.",
            },
            dimension_to_includes_excludes={
                PrisonsOffenseType.PERSON: IncludesExcludesSet(
                    members=PrisonPersonOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE
                    },
                ),
                PrisonsOffenseType.PROPERTY: IncludesExcludesSet(
                    members=PrisonPropertyOffenseIncludesExcludes,
                ),
                PrisonsOffenseType.PUBLIC_ORDER: IncludesExcludesSet(
                    members=PrisonPublicOrderOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_VIOLATIONS,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_EQUIPMENT_VIOLATIONS,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_SALES,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_DISTRIBUTION,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_MANUFACTURING,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_SMUGGLING,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_PRODUCTION,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_POSSESSION,
                    },
                ),
                PrisonsOffenseType.DRUG: IncludesExcludesSet(
                    members=PrisonDrugOffenseIncludesExcludes,
                ),
            },
        )
    ],
)

daily_population = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Daily Population",
    description="A single day count of the number of people incarcerated in the agency’s prison jurisdiction.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    specified_contexts=[],
    includes_excludes=IncludesExcludesSet(
        members=PopulationIncludesExcludes,
        excluded_set={
            PopulationIncludesExcludes.NOT_CONVICTED,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=BiologicalSex,
            required=True,
            dimension_to_description={
                BiologicalSex.MALE: "The number of people who are incarcerated under the agency’s prison jurisdiction whose biological sex is male.",
                BiologicalSex.FEMALE: "The number of people who are incarcerated under the agency’s prison jurisdiction whose biological sex is female.",
                BiologicalSex.UNKNOWN: "The number of people who are incarcerated under the agency’s prison jurisdiction whose biological sex is not known.",
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
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        AggregatedDimension(
            dimension=PrisonsOffenseType,
            required=True,
            dimension_to_description={
                PrisonsOffenseType.PERSON: "A single day count of the number of people incarcerated whose most serious offense was a crime against a person (the definition of person offenses configured in Section 2.2 will be applied to this section).",
                PrisonsOffenseType.PROPERTY: "A single day count of the number of people incarcerated whose most serious offense was a property crime (the definition of property offenses configured in Section 2.3 will be applied to this section).",
                PrisonsOffenseType.PUBLIC_ORDER: "A single day count of the number of people incarcerated whose most serious offense was a public order crime (the definition of public order offenses configured in Section 2.4 will be applied to this section).",
                PrisonsOffenseType.DRUG: "A single day count of the number of people incarcerated whose most serious offense was a drug crime (the definition of drug offenses configured in Section 2.5 will be applied to this section).",
                PrisonsOffenseType.OTHER: "A single day count of the number of people incarcerated whose most serious offense was not a person offense, property offense, public order offense, or drug offense (the definition of other offenses configured in Section 2.6 will be applied to this section).",
                PrisonsOffenseType.UNKNOWN: "A single day count of the number of people incarcerated whose most serious offense was an unknown crime.",
            },
            dimension_to_includes_excludes={
                PrisonsOffenseType.PERSON: IncludesExcludesSet(
                    members=PrisonPersonOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPersonOffenseIncludesExcludes.JUSTIFIABLE_HOMICIDE,
                    },
                ),
                PrisonsOffenseType.PROPERTY: IncludesExcludesSet(
                    members=PrisonPropertyOffenseIncludesExcludes,
                ),
                PrisonsOffenseType.DRUG: IncludesExcludesSet(
                    members=PrisonDrugOffenseIncludesExcludes,
                ),
                PrisonsOffenseType.PUBLIC_ORDER: IncludesExcludesSet(
                    members=PrisonPublicOrderOffenseIncludesExcludes,
                    excluded_set={
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_VIOLATIONS,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_EQUIPMENT_VIOLATIONS,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_SALES,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_DISTRIBUTION,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_MANUFACTURING,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_SMUGGLING,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_PRODUCTION,
                        PrisonPublicOrderOffenseIncludesExcludes.DRUG_POSSESSION,
                    },
                ),
            },
        ),
    ],
)

releases = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Releases",
    description="The number of release events from the agency’s prison jurisdiction to the community following a period of incarceration.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    reporting_note="Releases are based on the number of events in which a person was released from the jurisdiction of the agency, not the number of individual people released. If the same person was released from prison three times in a time period, it would count as three releases.",
    includes_excludes=IncludesExcludesSet(
        members=PrisonReleasesIncludesExcludes,
    ),
    specified_contexts=[],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=PrisonsReleaseType,
            required=False,
            dimension_to_description={
                PrisonsReleaseType.TO_PROBATION_SUPERVISION: "The number of release events from the agency’s prison jurisdiction to probation supervision.",
                PrisonsReleaseType.TO_PAROLE_SUPERVISION: "The number of release events from the agency’s prison jurisdiction to parole supervision.",
                PrisonsReleaseType.TO_COMMUNITY_SUPERVISION: "The number of release events from the agency’s prison jurisdiction to another form of community supervision that is not probation or parole or in the agency’s jurisdiction.",
                PrisonsReleaseType.NO_CONTROL: "The number of release events from the agency’s prison jurisdiction with no additional correctional control.",
                PrisonsReleaseType.DEATH: "The number of release events from the agency’s prison jurisdiction due to death of people in custody.",
                PrisonsReleaseType.OTHER: "The number of release events from the agency’s prison jurisdiction that are not releases to probation supervision, to parole supervision, to other community supervision, to no additional correctional control, or due to death.",
                PrisonsReleaseType.UNKNOWN: "The number of release events from the agency’s prison jurisdiction where the release type is not known.",
            },
            dimension_to_includes_excludes={
                PrisonsReleaseType.TO_PROBATION_SUPERVISION: IncludesExcludesSet(
                    members=PrisonReleasesToProbationIncludesExcludes,
                ),
                PrisonsReleaseType.TO_PAROLE_SUPERVISION: IncludesExcludesSet(
                    members=PrisonReleasesToParoleIncludesExcludes,
                ),
                PrisonsReleaseType.TO_COMMUNITY_SUPERVISION: IncludesExcludesSet(
                    members=PrisonReleasesCommunitySupervisionIncludesExcludes,
                ),
                PrisonsReleaseType.NO_CONTROL: IncludesExcludesSet(
                    members=PrisonReleasesNoControlIncludesExcludes,
                ),
                PrisonsReleaseType.DEATH: IncludesExcludesSet(
                    members=PrisonReleasesDeathIncludesExcludes,
                ),
            },
        )
    ],
)

staff_use_of_force_incidents = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.USE_OF_FORCE_INCIDENTS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Use of Force Incidents",
    description="The number of incidents in which agency staff use physical force to gain compliance from or control of a person who is under the agency’s jurisdiction.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Incidents represent unique events where force was used, not the number of people or staff involved in those events.",
    specified_contexts=[],
    definitions=[
        Definition(
            term="Use of force incident",
            definition="An event in which an officer uses force towards or in the vicinity of an individual incarcerated. The AJA focuses on uses of force resulting in injury or a discharge of a weapon.  Count all uses of force occurring during the same event as one incident.",
        )
    ],
    includes_excludes=IncludesExcludesSet(
        members=PrisonUseOfForceIncludesExcludes,
        excluded_set={PrisonUseOfForceIncludesExcludes.ROUTINE},
    ),
)

grievances_upheld = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.GRIEVANCES_UPHELD,
    category=MetricCategory.FAIRNESS,
    display_name="Grievances Upheld",
    description="The number of complaints from people who are incarcerated under the agency’s prison jurisdiction received via the process described in the institution’s grievance policy, which were resolved in a way that affirmed the complaint.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    reporting_note="Count grievances in the time period in which they were resolved, not when they were received or occurred. For instance, if a complaint was received on November 8, 2021, and resolved on January 14, 2022, that grievance would be counted in 2022.",
    specified_contexts=[],
    definitions=[
        Definition(
            term="Grievance",
            definition="A complaint or question filed with the institution by an individual incarcerated regarding their experience, with procedures, treatment, or interaction with officers.",
        )
    ],
    includes_excludes=IncludesExcludesSet(
        members=PrisonGrievancesIncludesExcludes,
        excluded_set={
            PrisonGrievancesIncludesExcludes.UNSUBSTANTIATED,
            PrisonGrievancesIncludesExcludes.DUPLICATE,
            PrisonGrievancesIncludesExcludes.PENDING_RESOLUTION,
            PrisonGrievancesIncludesExcludes.INFORMAL,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=GrievancesUpheldType,
            required=False,
            dimension_to_description={
                GrievancesUpheldType.LIVING_CONDITIONS: "The number of grievances upheld that relate to the living conditions of people who are incarcerated under the jurisdiction of the agency.",
                GrievancesUpheldType.PERSONAL_SAFETY: "The number of grievances upheld that relate to the personal safety of people who are incarcerated under the jurisdiction of the agency.",
                GrievancesUpheldType.DISCRIMINATION: "The number of grievances upheld that relate to acts of discrimination toward, racial bias against, or interference of religious practices of people who are incarcerated under the jurisdiction of the agency.",
                GrievancesUpheldType.ACCESS_TO_HEALTH_CARE: "The number of grievances upheld that relate to the accessibility of health care to people who are incarcerated under the jurisdiction of the agency.",
                GrievancesUpheldType.LEGAL: "The number of grievances upheld that relate to access to the legal process among people who are incarcerated under the jurisdiction of the agency.",
                GrievancesUpheldType.OTHER: "The number of grievances upheld that relate to another issue or concern that is not related to living conditions; personal safety; discrimination, racial bias, or religious practices; access to health care; or legal concerns.",
                GrievancesUpheldType.UNKNOWN: "The number of grievances upheld that relate to an issue or concern that is not known.",
            },
            dimension_to_includes_excludes={
                GrievancesUpheldType.LIVING_CONDITIONS: IncludesExcludesSet(
                    members=PrisonGrievancesLivingConditionsIncludesExcludes,
                ),
                GrievancesUpheldType.PERSONAL_SAFETY: IncludesExcludesSet(
                    members=PrisonGrievancesPersonalSafetyIncludesExcludes,
                ),
                GrievancesUpheldType.DISCRIMINATION: IncludesExcludesSet(
                    members=PrisonGrievancesDiscriminationIncludesExcludes,
                ),
                GrievancesUpheldType.ACCESS_TO_HEALTH_CARE: IncludesExcludesSet(
                    members=PrisonGrievancesHealthCareIncludesExcludes,
                ),
                GrievancesUpheldType.LEGAL: IncludesExcludesSet(
                    members=PrisonGrievancesLegalIncludesExcludes,
                ),
            },
        ),
    ],
)
