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
from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import BiologicalSex, RaceAndEthnicity
from recidiviz.justice_counts.dimensions.prisons import (
    ExpenseType,
    FundingType,
    GrievancesUpheldType,
    ReadmissionType,
    ReleaseType,
    StaffType,
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
from recidiviz.justice_counts.includes_excludes.prisons import (
    PopulationIncludesExcludes,
    PrisonAdmissionsIncludesExcludes,
    PrisonClinicalStaffIncludesExcludes,
    PrisonExpensesContractBedsIncludesExcludes,
    PrisonExpensesFacilitiesAndEquipmentIncludesExcludes,
    PrisonExpensesHealthCareIncludesExcludes,
    PrisonExpensesPersonnelIncludesExcludes,
    PrisonExpensesTimeframeAndSpendDownIncludesExcludes,
    PrisonExpensesTrainingIncludesExcludes,
    PrisonExpensesTypeIncludesExcludes,
    PrisonFundingPurposeIncludesExcludes,
    PrisonFundingTimeframeIncludesExcludes,
    PrisonGrievancesDiscriminationIncludesExcludes,
    PrisonGrievancesHealthCareIncludesExcludes,
    PrisonGrievancesIncludesExcludes,
    PrisonGrievancesLegalIncludesExcludes,
    PrisonGrievancesLivingConditionsIncludesExcludes,
    PrisonGrievancesPersonalSafetyIncludesExcludes,
    PrisonManagementAndOperationsStaffIncludesExcludes,
    PrisonProgrammaticStaffIncludesExcludes,
    PrisonReadmissionsNewConvictionIncludesExcludes,
    PrisonReadmissionsOtherCommunitySupervisionIncludesExcludes,
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
    system=System.PRISONS,
    metric_type=MetricType.FUNDING,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    unit=MetricUnit.AMOUNT,
    includes_excludes=[
        IncludesExcludesSet(
            members=PrisonFundingTimeframeIncludesExcludes,
            description="Funding timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=PrisonFundingPurposeIncludesExcludes,
            description="Funding purpose",
            excluded_set={
                PrisonFundingPurposeIncludesExcludes.JAIL_OPERATIONS,
                PrisonFundingPurposeIncludesExcludes.NON_PRISON_ACTIVITIES,
                PrisonFundingPurposeIncludesExcludes.JUVENILE_JAILS,
                PrisonFundingPurposeIncludesExcludes.LAW_ENFORCEMENT,
            },
        ),
    ],
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=False,
            dimension_to_description={
                FundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                FundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                FundingType.COMMISSARY_AND_FEES: "The amount of funding the agency collected through sales and/or fees charged to people who are incarcerated under the jurisdiction of the agency or their visitors.",
                FundingType.CONTRACT_BEDS: "The amount of funding the agency collected through contracts to provide custody and care for people who are incarcerated under the jurisdiction of another agency.",
                FundingType.OTHER: "The amount of funding for the operation and maintenance of prison facilities and the care of people who are incarcerated that is not appropriated by the state, funded through grants, earned from commissary and fees, or collected from contracted beds.",
                FundingType.UNKNOWN: "The amount of funding for the operation and maintenance of prison facilities and the care of people who are incarcerated for which the source is not known.",
            },
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATION: [
                    IncludesExcludesSet(
                        members=PrisonsFundingStateAppropriationIncludesExcludes,
                        excluded_set={
                            PrisonsFundingStateAppropriationIncludesExcludes.PROPOSED,
                            PrisonsFundingStateAppropriationIncludesExcludes.PRELIMINARY,
                            PrisonsFundingStateAppropriationIncludesExcludes.GRANTS,
                        },
                    ),
                ],
                FundingType.GRANTS: [
                    IncludesExcludesSet(
                        members=PrisonsFundingGrantsIncludesExcludes,
                    ),
                ],
                FundingType.COMMISSARY_AND_FEES: [
                    IncludesExcludesSet(
                        members=PrisonsFundingCommissaryAndFeesIncludesExcludes,
                    ),
                ],
                FundingType.CONTRACT_BEDS: [
                    IncludesExcludesSet(
                        members=PrisonsFundingContractBedsIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)

staff = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the agency for the operation and maintenance of the prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    unit=MetricUnit.FULL_TIME,
    includes_excludes=[
        IncludesExcludesSet(
            members=PrisonStaffIncludesExcludes,
            excluded_set={
                PrisonStaffIncludesExcludes.VOLUNTEER,
                PrisonStaffIncludesExcludes.INTERN,
            },
        ),
    ],
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_description={
                StaffType.SECURITY: "The number of full-time equivalent positions that work directly with people who are incarcerated and are responsible for their custody, supervision, and monitoring.",
                StaffType.MANAGEMENT_AND_OPERATIONS: "The number of full-time equivalent positions that do not work directly with people who are incarcerated, but support the day-to-day operations of the agency.",
                StaffType.CLINICAL_AND_MEDICAL: "The number of full-time equivalent positions that work directly with people who are incarcerated and are responsible for their health.",
                StaffType.PROGRAMMATIC: "The number of full-time equivalent positions that are not medical or clinical staff that provide services and programming to people who are incarcerated.",
                StaffType.OTHER: "The number of full-time equivalent positions dedicated to the operation and maintenance of prison facilities under the jurisdiction of the agency that are not security staff, management and operations staff, clinical and medical staff, or programmatic staff.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions dedicated to the operation and maintenance of prison facilities under the jurisdiction of the agency that are of an unknown type.",
                StaffType.VACANT: "The number of full-time equivalent positions dedicated to the operation and maintenance of prison facilities under the jurisdiction of the agency of any type that are budgeted but not currently filled.",
            },
            dimension_to_includes_excludes={
                StaffType.SECURITY: [
                    IncludesExcludesSet(
                        members=PrisonSecurityStaffIncludesExcludes,
                        excluded_set={
                            PrisonSecurityStaffIncludesExcludes.VACANT,
                        },
                    ),
                ],
                StaffType.MANAGEMENT_AND_OPERATIONS: [
                    IncludesExcludesSet(
                        members=PrisonManagementAndOperationsStaffIncludesExcludes,
                        excluded_set={
                            PrisonManagementAndOperationsStaffIncludesExcludes.VACANT,
                        },
                    ),
                ],
                StaffType.CLINICAL_AND_MEDICAL: [
                    IncludesExcludesSet(
                        members=PrisonClinicalStaffIncludesExcludes,
                        excluded_set={
                            PrisonClinicalStaffIncludesExcludes.VACANT,
                        },
                    ),
                ],
                StaffType.PROGRAMMATIC: [
                    IncludesExcludesSet(
                        members=PrisonProgrammaticStaffIncludesExcludes,
                        excluded_set={
                            PrisonProgrammaticStaffIncludesExcludes.VACANT,
                            PrisonProgrammaticStaffIncludesExcludes.VOLUNTEER,
                        },
                    ),
                ],
                StaffType.VACANT: [
                    IncludesExcludesSet(
                        members=VacantPrisonStaffIncludesExcludes,
                        excluded_set={
                            VacantPrisonStaffIncludesExcludes.FILLED,
                        },
                    ),
                ],
            },
        )
    ],
)

expenses = MetricDefinition(
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    system=System.PRISONS,
    description="The amount spent by the agency for the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
    unit=MetricUnit.AMOUNT,
    metric_type=MetricType.EXPENSES,
    display_name="Expenses",
    measurement_type=MeasurementType.DELTA,
    category=MetricCategory.CAPACITY_AND_COST,
    includes_excludes=[
        IncludesExcludesSet(
            members=PrisonExpensesTimeframeAndSpendDownIncludesExcludes,
            description="Expenses timeframe and spend-down",
        ),
        IncludesExcludesSet(
            members=PrisonExpensesTypeIncludesExcludes,
            description="Expense type",
            excluded_set={
                PrisonExpensesTypeIncludesExcludes.JAIL_FACILITY,
                PrisonExpensesTypeIncludesExcludes.JUVENILE_JAIL,
                PrisonExpensesTypeIncludesExcludes.LAW_ENFORCEMENT,
                PrisonExpensesTypeIncludesExcludes.NON_PRISON_ACTIVITIES,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            required=False,
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount spent by the agency to employ personnel involved in the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.TRAINING: "The amount spent by the agency on the training of personnel involved in the operation and maintenance of prison facilities and the care of people who are incarcerated under the jurisdiction of the agency, including any associated expenses, such as registration fees and travel costs.",
                ExpenseType.FACILITIES_AND_EQUIPMENT: "The amount spent by the agency for the purchase and use of the physical plant, property owned and operated by the agency, and equipment used to support maintenance of prison facilities and care of people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.HEALTH_CARE: "The amount spent by the agency on medical care for people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.CONTRACT_BEDS: "The amount spent by the agency on contracts with other agencies to provide custody and care for people who are incarcerated under the jurisdiction of the agency.",
                ExpenseType.OTHER: "The amount spent by the agency on other costs relating to the operation and maintenance of prison facilities and the care of people who are incarcerated that are not personnel, training, facilities and equipment, health care, or contract bed expenses.",
                ExpenseType.UNKNOWN: "The amount spent by the agency on costs relating to the operation and maintenance of prison facilities and the care of people who are incarcerated for a purpose that is not known.",
            },
            dimension_to_includes_excludes={
                ExpenseType.PERSONNEL: [
                    IncludesExcludesSet(
                        members=PrisonExpensesPersonnelIncludesExcludes,
                        excluded_set={
                            PrisonExpensesPersonnelIncludesExcludes.COMPANIES_AND_SERVICES
                        },
                    ),
                ],
                ExpenseType.TRAINING: [
                    IncludesExcludesSet(
                        members=PrisonExpensesTrainingIncludesExcludes,
                        excluded_set={PrisonExpensesTrainingIncludesExcludes.NO_COST},
                    ),
                ],
                ExpenseType.FACILITIES_AND_EQUIPMENT: [
                    IncludesExcludesSet(
                        members=PrisonExpensesFacilitiesAndEquipmentIncludesExcludes,
                    ),
                ],
                ExpenseType.HEALTH_CARE: [
                    IncludesExcludesSet(
                        members=PrisonExpensesHealthCareIncludesExcludes,
                        excluded_set={
                            PrisonExpensesHealthCareIncludesExcludes.TRANSPORTATION
                        },
                    ),
                ],
                ExpenseType.CONTRACT_BEDS: [
                    IncludesExcludesSet(
                        members=PrisonExpensesContractBedsIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)

readmissions = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.READMISSIONS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Readmissions",
    description="The number of admission events to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within the previous three years (1,096 days).",
    additional_description="For instance, if a person admitted on June 23, 2022, had been incarcerated at any time between June 23, 2019, and June 22, 2022, it would count as a readmission. This metric is based on admission events, so if the same person is readmitted three times in a time period, it would count as three readmissions.",
    unit=MetricUnit.READMISSIONS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    # TODO(#18071) Implement repeated/reused includes/excludes
    includes_excludes=[
        IncludesExcludesSet(
            members=PrisonAdmissionsIncludesExcludes,
            excluded_set={
                PrisonAdmissionsIncludesExcludes.TEMPORARY_ABSENCES,
                PrisonAdmissionsIncludesExcludes.TRANSER_SAME_STATE,
                PrisonAdmissionsIncludesExcludes.FEDERAL_HOLD_US_MARSHALS_SERVICE,
                PrisonAdmissionsIncludesExcludes.FEDERAL_HOLD_TRIBAL,
                PrisonAdmissionsIncludesExcludes.AWAITING_HEARINGS,
                PrisonAdmissionsIncludesExcludes.FAILURE_TO_PAY,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ReadmissionType,
            required=False,
            dimension_to_description={
                ReadmissionType.NEW_CONVICTION: "The number of reincarceration events due to new criminal conviction and sentence to prison of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                ReadmissionType.RETURN_FROM_PROBATION: "The number of reincarceration events due to probation hold, sanction, or revocation to prison of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                ReadmissionType.RETURN_FROM_PAROLE: "The number of reincarceration events due to parole hold, sanction, or revocation to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                ReadmissionType.OTHER_COMMUNITY_SUPERVISION: "The number of reincarceration events due to other community supervision hold, sanction, or revocation to prison of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                ReadmissionType.OTHER: "The number of reincarceration events which were not admissions for a new conviction, admissions for a return from probation, or admissions for a return from parole, but another admission to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
                ReadmissionType.UNKNOWN: "The number of reincarceration events for an unknown reason, to the agency’s prison jurisdiction of people who were incarcerated in the agency’s jurisdiction within three years (1,096 days) prior to their current admission.",
            },
            dimension_to_includes_excludes={
                ReadmissionType.NEW_CONVICTION: [
                    IncludesExcludesSet(
                        members=PrisonReadmissionsNewConvictionIncludesExcludes,
                        excluded_set={
                            PrisonReadmissionsNewConvictionIncludesExcludes.REVOKED_NEW_OFFENSE,
                            PrisonReadmissionsNewConvictionIncludesExcludes.REVOKED_NEW_CONVICTION,
                        },
                    ),
                ],
                ReadmissionType.RETURN_FROM_PROBATION: [
                    IncludesExcludesSet(
                        members=PrisonReadmissionsProbationIncludesExcludes,
                    ),
                ],
                ReadmissionType.RETURN_FROM_PAROLE: [
                    IncludesExcludesSet(
                        members=PrisonReadmissionsParoleIncludesExcludes,
                    ),
                ],
                ReadmissionType.OTHER_COMMUNITY_SUPERVISION: [
                    IncludesExcludesSet(
                        members=PrisonReadmissionsOtherCommunitySupervisionIncludesExcludes,
                    ),
                ],
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
    additional_description="""Admissions are based on the number of events in which a person was incarcerated in a prison facility, not the number of individual people who entered the facility. If the same person was admitted to prison three times in a time period, it would count as three admissions.

For people who were convicted of multiple offenses, a hierarchy rule determines how they are presented in this analysis. Justice Counts recommends following the summary reporting model of crimes against persons considered most serious, followed by crimes against property, public order offenses,  drug offenses, other offenses, and unknown offenses.""",
    unit=MetricUnit.ADMISSIONS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=PrisonAdmissionsIncludesExcludes,
            excluded_set={
                PrisonAdmissionsIncludesExcludes.TEMPORARY_ABSENCES,
                PrisonAdmissionsIncludesExcludes.TRANSER_SAME_STATE,
                PrisonAdmissionsIncludesExcludes.FEDERAL_HOLD_US_MARSHALS_SERVICE,
                PrisonAdmissionsIncludesExcludes.FEDERAL_HOLD_TRIBAL,
                PrisonAdmissionsIncludesExcludes.AWAITING_HEARINGS,
                PrisonAdmissionsIncludesExcludes.FAILURE_TO_PAY,
            },
        ),
    ],
    # TODO(#18071) Implement repeated/reused includes/excludes
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
            dimension_to_description={
                OffenseType.PERSON: "The number of admission events to the jurisdiction of the prison agency for which the most serious offense was a crime against a person.",
                OffenseType.PROPERTY: "The number of admission events to the jurisdiction of the prison agency for which the most serious offense was a property offense.",
                OffenseType.PUBLIC_ORDER: "The number of admission events to the jurisdiction of the prison agency for which the most serious offense was a public order offense.",
                OffenseType.DRUG: "The number of admission events to the jurisdiction of the prison agency for which the most serious offense was a drug offense.",
                OffenseType.OTHER: "The number of admission events to jurisdiction of the prison agency for which the most serious offense was for another type of offense that was not a person offense, a property offense, a drug offense, or a public order offense.",
                OffenseType.UNKNOWN: "The number of admission events to the jurisdiction of the prison agency for which the most serious offense is not known.",
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
                    IncludesExcludesSet(
                        members=DrugOffenseIncludesExcludes,
                    ),
                ],
            },
        )
    ],
)

daily_population = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Daily Population",
    description="A single day count of the number of people incarcerated under the jurisdiction of the prison agency.",
    additional_description="For people who were convicted of multiple offenses, a hierarchy rule determines how they are presented in this analysis. Justice Counts recommends following the summary reporting model of crimes against persons considered most serious, followed by crimes against property, public order offenses, drug offenses, other offenses, and unknown offenses.",
    unit=MetricUnit.PEOPLE_INCARCERATED,
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=PopulationIncludesExcludes,
            excluded_set={
                PopulationIncludesExcludes.NOT_CONVICTED,
            },
        ),
    ],
    aggregated_dimensions=[
        # TODO(#18071) Implement repeated/reused global includes/excludes
        # TODO(#17579) Implement Y/N tables
        AggregatedDimension(
            dimension=BiologicalSex,
            required=True,
            dimension_to_description={
                BiologicalSex.MALE: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is male.",
                BiologicalSex.FEMALE: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is female.",
                BiologicalSex.UNKNOWN: "A single day count of the number of people who are incarcerated under the jurisdiction of the prison agency whose biological sex is not known.",
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
        # TODO(#18071) Implement repeated/reused global includes/excludes
        # TODO(#17579) Implement Y/N tables
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),
        # TODO(#18071) Implement repeated/reused global includes/excludes
        AggregatedDimension(
            dimension=OffenseType,
            required=True,
            dimension_to_description={
                OffenseType.PERSON: "A single day count of the number of people incarcerated under the jurisdiction of the prison agency whose most serious offense was an offense against a person.",
                OffenseType.PROPERTY: "A single day count of the number of people incarcerated under the jurisdiction of the prison agency whose most serious offense was a property offense.",
                OffenseType.PUBLIC_ORDER: "A single day count of the number of people incarcerated under the jurisdiction of the prison agency whose most serious offense was a public order offense.",
                OffenseType.DRUG: "A single day count of the number of people incarcerated under the jurisdiction of the prison agency whose most serious offense was a drug offense.",
                OffenseType.OTHER: "A single day count of the number of people incarcerated under the jurisdiction of the prison agency whose most serious offense was not a person offense, property offense, public order offense, or drug offense.",
                OffenseType.UNKNOWN: "A single day count of the number of people incarcerated under the jurisdiction of the prison agency whose most serious offense was unknown.",
            },
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
                        excluded_set={PropertyOffenseIncludesExcludes.ROBBERY},
                    ),
                ],
                OffenseType.DRUG: [
                    IncludesExcludesSet(
                        members=DrugOffenseIncludesExcludes,
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
            },
        ),
    ],
)

releases = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.RELEASES,
    category=MetricCategory.POPULATIONS,
    display_name="Releases",
    description="The number of release events from the jurisdiction of the prison agency following a period of incarceration.",
    unit=MetricUnit.RELEASES,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=[
        IncludesExcludesSet(
            members=PrisonReleasesIncludesExcludes,
            excluded_set={
                PrisonReleasesIncludesExcludes.TRANSFERRED_WITHIN,
                PrisonReleasesIncludesExcludes.TEMPORARILY_TRANSFERRED,
                PrisonReleasesIncludesExcludes.TEMPORARILY_ABSENT,
            },
        ),
    ],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ReleaseType,
            required=False,
            dimension_to_description={
                ReleaseType.TO_PROBATION_SUPERVISION: "The number of release events from the agency’s prison jurisdiction to probation supervision.",
                ReleaseType.TO_PAROLE_SUPERVISION: "The number of release events from the agency’s prison jurisdiction to parole supervision.",
                ReleaseType.TO_COMMUNITY_SUPERVISION: "The number of release events from the agency’s prison jurisdiction to another form of community supervision that is not probation or parole.",
                ReleaseType.NO_CONTROL: "The number of release events from the agency’s prison jurisdiction with no additional correctional control.",
                ReleaseType.DEATH: "The number of release events from the agency’s prison jurisdiction due to death of people in its jurisdiction.",
                ReleaseType.OTHER: "The number of release events from the agency’s prison jurisdiction that are not releases to probation supervision, to parole supervision, to other community supervision, to no additional correctional control, or due to death.",
                ReleaseType.UNKNOWN: "The number of release events from the agency’s prison jurisdiction where the release type is not known.",
            },
            # TODO(#18071) Implement repeated/reused global includes/excludes
            dimension_to_includes_excludes={
                ReleaseType.TO_PROBATION_SUPERVISION: [
                    IncludesExcludesSet(
                        members=PrisonReleasesToProbationIncludesExcludes,
                    ),
                ],
                ReleaseType.TO_PAROLE_SUPERVISION: [
                    IncludesExcludesSet(
                        members=PrisonReleasesToParoleIncludesExcludes,
                    ),
                ],
                ReleaseType.TO_COMMUNITY_SUPERVISION: [
                    IncludesExcludesSet(
                        members=PrisonReleasesCommunitySupervisionIncludesExcludes,
                    ),
                ],
                ReleaseType.NO_CONTROL: [
                    IncludesExcludesSet(
                        members=PrisonReleasesNoControlIncludesExcludes,
                    ),
                ],
                ReleaseType.DEATH: [
                    IncludesExcludesSet(
                        members=PrisonReleasesDeathIncludesExcludes,
                    ),
                ],
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
    additional_description="Incidents represent unique events where force was used, not the number of people or staff involved in those events.",
    unit=MetricUnit.INCIDENTS,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=PrisonUseOfForceIncludesExcludes,
            excluded_set={PrisonUseOfForceIncludesExcludes.ROUTINE},
        ),
    ],
)

grievances_upheld = MetricDefinition(
    system=System.PRISONS,
    metric_type=MetricType.GRIEVANCES_UPHELD,
    category=MetricCategory.FAIRNESS,
    display_name="Grievances Upheld",
    description="The number of complaints from people who are incarcerated under the agency’s prison jurisdiction received via the process described in the institution’s grievance policy, which were resolved in a way that affirmed the complaint.",
    additional_description="Count grievances in the time period in which they were resolved, not when they were received or occurred. For instance, if a complaint was received on November 8, 2021, and resolved on January 14, 2022, that grievance would be counted in 2022.",
    unit=MetricUnit.GRIEVANCES_UPHELD,
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=[
        IncludesExcludesSet(
            members=PrisonGrievancesIncludesExcludes,
            excluded_set={
                PrisonGrievancesIncludesExcludes.UNSUBSTANTIATED,
                PrisonGrievancesIncludesExcludes.PENDING_RESOLUTION,
                PrisonGrievancesIncludesExcludes.INFORMAL,
                PrisonGrievancesIncludesExcludes.DUPLICATE,
                PrisonGrievancesIncludesExcludes.FILED_BY_VISITOR,
                PrisonGrievancesIncludesExcludes.FILED_BY_STAFF,
            },
        ),
    ],
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
                GrievancesUpheldType.LIVING_CONDITIONS: [
                    IncludesExcludesSet(
                        members=PrisonGrievancesLivingConditionsIncludesExcludes,
                    ),
                ],
                GrievancesUpheldType.PERSONAL_SAFETY: [
                    IncludesExcludesSet(
                        members=PrisonGrievancesPersonalSafetyIncludesExcludes,
                    ),
                ],
                GrievancesUpheldType.DISCRIMINATION: [
                    IncludesExcludesSet(
                        members=PrisonGrievancesDiscriminationIncludesExcludes,
                    ),
                ],
                GrievancesUpheldType.ACCESS_TO_HEALTH_CARE: [
                    IncludesExcludesSet(
                        members=PrisonGrievancesHealthCareIncludesExcludes,
                    ),
                ],
                GrievancesUpheldType.LEGAL: [
                    IncludesExcludesSet(
                        members=PrisonGrievancesLegalIncludesExcludes,
                    ),
                ],
            },
        ),
    ],
)
