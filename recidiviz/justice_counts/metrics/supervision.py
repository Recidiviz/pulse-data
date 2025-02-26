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
"""Defines all Justice Counts metrics for Supervision."""

from recidiviz.justice_counts.dimensions.offense import OffenseType
from recidiviz.justice_counts.dimensions.person import (
    BiologicalSex,
    GenderRestricted,
    RaceAndEthnicity,
)
from recidiviz.justice_counts.dimensions.supervision import (
    DailyPopulationType,
    DischargeType,
    ExpenseType,
    FundingType,
    RevocationType,
    StaffType,
    ViolationType,
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
from recidiviz.justice_counts.includes_excludes.supervision import (
    PeopleAbscondedSupervisionIncludesExcludes,
    PeopleIncarceratedOnHoldSanctionSupervisionIncludesExcludes,
    PeopleOnActiveSupervisionIncludesExcludes,
    PeopleOnAdministrativeSupervisionIncludesExcludes,
    SupervisionAbscondingViolationsIncludesExcludes,
    SupervisionCaseloadDenominatorIncludesExcludes,
    SupervisionCaseloadNumeratorIncludesExcludes,
    SupervisionClinicalMedicalStaffIncludesExcludes,
    SupervisionCountyMunicipalAppropriationIncludesExcludes,
    SupervisionDischargesIncludesExcludes,
    SupervisionExpensesIncludesExcludes,
    SupervisionFacilitiesEquipmentExpensesIncludesExcludes,
    SupervisionFinesFeesIncludesExcludes,
    SupervisionFundingIncludesExcludes,
    SupervisionGrantsIncludesExcludes,
    SupervisionManagementOperationsStaffIncludesExcludes,
    SupervisionNeutralDischargeIncludesExcludes,
    SupervisionNewCasesIncludesExcludes,
    SupervisionNewOffenseViolationsIncludesExcludes,
    SupervisionPersonnelExpensesIncludesExcludes,
    SupervisionProgrammaticStaffIncludesExcludes,
    SupervisionReconvictionsIncludesExcludes,
    SupervisionRevocationsIncludesExcludes,
    SupervisionStaffDimIncludesExcludes,
    SupervisionStaffIncludesExcludes,
    SupervisionStateAppropriationIncludesExcludes,
    SupervisionSuccessfulCompletionIncludesExcludes,
    SupervisionTechnicalViolationsIncludesExcludes,
    SupervisionTrainingExpensesIncludesExcludes,
    SupervisionUnsuccessfulDischargeIncludesExcludes,
    SupervisionVacantStaffIncludesExcludes,
    SupervisionViolationsIncludesExcludes,
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
    system=System.SUPERVISION,
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
    system=System.SUPERVISION,
    metric_type=MetricType.BUDGET,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Funding",
    description="The amount of funding for the provision of community supervision and operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        # TODO(#17577)
        members=SupervisionFundingIncludesExcludes,
        excluded_set={
            SupervisionFundingIncludesExcludes.STIPENDS_JAIL,
            SupervisionFundingIncludesExcludes.STIPENDS_PRISON,
            SupervisionFundingIncludesExcludes.JAIL_MAINTENANCE,
            SupervisionFundingIncludesExcludes.PRISON_MAINTENANCE,
            SupervisionFundingIncludesExcludes.JUVENILE_SUPERVISION,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=FundingType,
            required=False,
            dimension_to_includes_excludes={
                FundingType.STATE_APPROPRIATION: IncludesExcludesSet(
                    members=SupervisionStateAppropriationIncludesExcludes,
                    excluded_set={
                        SupervisionStateAppropriationIncludesExcludes.PROPOSED,
                        SupervisionStateAppropriationIncludesExcludes.PRELIMINARY,
                        SupervisionStateAppropriationIncludesExcludes.GRANTS_NOT_BUDGET,
                    },
                ),
                FundingType.COUNTY_MUNICIPAL_APPROPRIATION: IncludesExcludesSet(
                    members=SupervisionCountyMunicipalAppropriationIncludesExcludes,
                    excluded_set={
                        SupervisionCountyMunicipalAppropriationIncludesExcludes.PROPOSED,
                        SupervisionCountyMunicipalAppropriationIncludesExcludes.PRELIMINARY,
                    },
                ),
                FundingType.GRANTS: IncludesExcludesSet(
                    members=SupervisionGrantsIncludesExcludes,
                ),
                FundingType.FINES_FEES: IncludesExcludesSet(
                    members=SupervisionFinesFeesIncludesExcludes,
                    excluded_set={
                        SupervisionFinesFeesIncludesExcludes.RESTITUTION,
                        SupervisionFinesFeesIncludesExcludes.LEGAL_OBLIGATIONS,
                    },
                ),
            },
            dimension_to_description={
                FundingType.STATE_APPROPRIATION: "The amount of funding appropriated by the state for the provision of community supervision and the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
                FundingType.COUNTY_MUNICIPAL_APPROPRIATION: "The amount of funding appropriated by counties or cities for the provision of community supervision and the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
                FundingType.GRANTS: "The amount of funding derived by the agency through grants and awards to be used for the provision of community supervision and the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
                FundingType.FINES_FEES: "The amount of funding the agency collected from people on supervision that is used to support the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
                FundingType.OTHER: "The amount of funding for the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency that is not appropriations from the state, appropriations from the county or municipality, funding from grants, or funding from fines or fees.",
                FundingType.UNKNOWN: "The amount of funding for the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency for which the source is not known.",
            },
        )
    ],
)

expenses = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.EXPENSES,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Expenses",
    description="The amount spent by the agency for the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        # TODO(#17577)
        members=SupervisionExpensesIncludesExcludes,
        excluded_set={
            SupervisionExpensesIncludesExcludes.STIPENDS_JAILS,
            SupervisionExpensesIncludesExcludes.STIPENDS_PRISONS,
            SupervisionExpensesIncludesExcludes.JAILS,
            SupervisionExpensesIncludesExcludes.PRISONS,
            SupervisionExpensesIncludesExcludes.JUVENILE_SUPERVISION,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ExpenseType,
            required=True,
            dimension_to_includes_excludes={
                ExpenseType.PERSONNEL: IncludesExcludesSet(
                    members=SupervisionPersonnelExpensesIncludesExcludes,
                    excluded_set={
                        SupervisionPersonnelExpensesIncludesExcludes.COMPANIES_CONTRACTED,
                    },
                ),
                ExpenseType.TRAINING: IncludesExcludesSet(
                    members=SupervisionTrainingExpensesIncludesExcludes,
                ),
                ExpenseType.FACILITIES_EQUIPMENT: IncludesExcludesSet(
                    members=SupervisionFacilitiesEquipmentExpensesIncludesExcludes,
                ),
            },
            dimension_to_description={
                ExpenseType.PERSONNEL: "The amount spent by the agency to employ personnel involved in the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
                ExpenseType.TRAINING: "The amount spent by the agency on the training of personnel involved in the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency, including any associated expenses, such as registration fees and travel costs.",
                ExpenseType.FACILITIES_EQUIPMENT: "The amount spent by the agency for the purchase and use of the physical plant and property owned and operated by the agency for the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
                ExpenseType.OTHER: "The amount spent by the agency on other costs relating to the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency that are not personnel, training, or facilities and equipment expenses.",
                ExpenseType.UNKNOWN: "The amount spent by the agency on other costs relating to the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency for a purpose that is not known.",
            },
        ),
    ],
)

total_staff = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.TOTAL_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff",
    description="The number of full-time equivalent positions budgeted for the agency for the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=StaffType,
            required=False,
            dimension_to_includes_excludes={
                StaffType.SUPERVISION: IncludesExcludesSet(
                    members=SupervisionStaffDimIncludesExcludes,
                    excluded_set={SupervisionStaffDimIncludesExcludes.VACANT},
                ),
                StaffType.MANAGEMENT_AND_OPERATIONS: IncludesExcludesSet(
                    members=SupervisionManagementOperationsStaffIncludesExcludes,
                    excluded_set={
                        SupervisionManagementOperationsStaffIncludesExcludes.VACANT
                    },
                ),
                StaffType.CLINICAL_AND_MEDICAL: IncludesExcludesSet(
                    members=SupervisionClinicalMedicalStaffIncludesExcludes,
                    excluded_set={
                        SupervisionClinicalMedicalStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.PROGRAMMATIC: IncludesExcludesSet(
                    members=SupervisionProgrammaticStaffIncludesExcludes,
                    excluded_set={
                        SupervisionProgrammaticStaffIncludesExcludes.VOLUNTEER,
                        SupervisionProgrammaticStaffIncludesExcludes.VACANT,
                    },
                ),
                StaffType.VACANT: IncludesExcludesSet(
                    members=SupervisionVacantStaffIncludesExcludes,
                    excluded_set={
                        SupervisionVacantStaffIncludesExcludes.FILLED,
                    },
                ),
            },
            dimension_to_description={
                StaffType.SUPERVISION: "The number of full-time equivalent positions that work directly with people who are on supervision and are responsible for their supervision and case management.",
                StaffType.MANAGEMENT_AND_OPERATIONS: "The number of full-time equivalent positions that do not work directly with people who are supervised in the community but support the day-to-day operations of the supervision agency.",
                StaffType.CLINICAL_AND_MEDICAL: "The number of full-time equivalent positions that work directly with people on probation, parole, or other community supervision and are responsible for their physical or mental health.",
                StaffType.PROGRAMMATIC: "The number of full-time equivalent positions that provide services and programming to people on community supervision but are not medical or clinical staff.",
                StaffType.OTHER: "The number of full-time equivalent positions dedicated to the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency that are not supervision staff, management and operations staff, clinical and medical staff, or programmatic staff.",
                StaffType.UNKNOWN: "The number of full-time equivalent positions dedicated to the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency that are of an unknown type.",
                StaffType.VACANT: "The number of full-time equivalent positions of any type dedicated to the provision of community supervision or the operation and maintenance of community supervision facilities under the jurisdiction of the agency that are budgeted but not currently filled.",
            },
        ),
    ],
    includes_excludes=IncludesExcludesSet(
        members=SupervisionStaffIncludesExcludes,
        excluded_set={
            SupervisionStaffIncludesExcludes.VOLUNTEER,
            SupervisionStaffIncludesExcludes.INTERN,
        },
    ),
)

violations = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.SUPERVISION_VIOLATIONS,
    category=MetricCategory.OPERATIONS_AND_DYNAMICS,
    display_name="Violations",
    description="The number of incidents in which conditions of supervision were violated. Incidents may include multiple violations that are reported by the agency at the same time, commonly called violation reports.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=SupervisionViolationsIncludesExcludes,
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=ViolationType,
            required=True,
            dimension_to_includes_excludes={
                ViolationType.TECHNICAL: IncludesExcludesSet(
                    members=SupervisionTechnicalViolationsIncludesExcludes,
                    excluded_set={
                        SupervisionTechnicalViolationsIncludesExcludes.CRIMINAL_OFFENSE,
                        SupervisionTechnicalViolationsIncludesExcludes.ARREST,
                        SupervisionTechnicalViolationsIncludesExcludes.CONVICTION,
                        SupervisionTechnicalViolationsIncludesExcludes.ABSCONDING,
                    },
                ),
                ViolationType.ABSCONDING: IncludesExcludesSet(
                    members=SupervisionAbscondingViolationsIncludesExcludes,
                ),
                ViolationType.NEW_OFFENSE: IncludesExcludesSet(
                    members=SupervisionNewOffenseViolationsIncludesExcludes,
                ),
            },
            dimension_to_description={
                ViolationType.TECHNICAL: "The number of people who violated conditions of supervision in which the most serious violation was defined as “technical” within the supervision agency.",
                ViolationType.ABSCONDING: "The number of people who violated conditions of supervision in which the most serious violation was defined as “absconding” within the supervision agency.",
                ViolationType.NEW_OFFENSE: "The number of people who violated conditions of supervision in which the most serious violation was defined as “new offense” within the supervision agency.",
                ViolationType.OTHER: "The number of people who violated conditions of supervision in which the most serious violation was not covered in technical violations, absconding, or new offenses.",
                ViolationType.UNKNOWN: "The number of people who violated an unknown condition of supervision.",
            },
        )
    ],
)

new_cases = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.SUPERVISION_STARTS,
    category=MetricCategory.POPULATIONS,
    display_name="New Cases",
    description="The number of people with new community supervision cases referred to the agency as the result of a legal decision made by the courts or another authority, such as a parole board.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=SupervisionNewCasesIncludesExcludes,
        excluded_set={
            SupervisionNewCasesIncludesExcludes.TRANSFERRED,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=OffenseType,
            required=False,
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
                    members=DrugOffenseIncludesExcludes,
                ),
            },
            dimension_to_description={
                OffenseType.PERSON: "The number of people with new community supervision cases referred to the agency in which the most serious originating offense was a crime against a person.",
                OffenseType.PROPERTY: "The number of people with new community supervision cases referred to the agency in which the most serious originating offense was a property offense.",
                OffenseType.PUBLIC_ORDER: "The number of people with new community supervision cases referred to the agency in which the most serious originating offense was a public order offense.",
                OffenseType.DRUG: "The number of people with new community supervision cases referred to the agency in which the most serious originating offense was a drug offense.",
                OffenseType.OTHER: "The number of people with new community supervision cases referred to the agency in which the most serious originating charge/offense was another type of crime that was not a person, property, drug, or public order charge/offense.",
                OffenseType.UNKNOWN: "The number of people with arrests, citations, or summons made by the agency in which the most serious charge/offense is not known.",
            },
        )
    ],
)

daily_population = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.POPULATION,
    category=MetricCategory.POPULATIONS,
    display_name="Daily Population",
    description="A single day count of the number of people who are supervised under the jurisdiction of the agency.",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=DailyPopulationType,
            required=True,
            dimension_to_includes_excludes={
                DailyPopulationType.ACTIVE: IncludesExcludesSet(
                    members=PeopleOnActiveSupervisionIncludesExcludes,
                    excluded_set={
                        PeopleOnActiveSupervisionIncludesExcludes.TELEPHONE_MAIL_CONTACTS
                    },
                ),
                DailyPopulationType.ADMINISTRATIVE: IncludesExcludesSet(
                    members=PeopleOnAdministrativeSupervisionIncludesExcludes,
                ),
                DailyPopulationType.ABSCONDED: IncludesExcludesSet(
                    members=PeopleAbscondedSupervisionIncludesExcludes,
                ),
                DailyPopulationType.HOLD_OR_SANCTION: IncludesExcludesSet(
                    members=PeopleIncarceratedOnHoldSanctionSupervisionIncludesExcludes,
                    excluded_set={
                        PeopleIncarceratedOnHoldSanctionSupervisionIncludesExcludes.REVOKED_TO_PRISON_JAIL,
                    },
                ),
            },
            dimension_to_description={
                DailyPopulationType.ACTIVE: "The number of people who are supervised by the agency on active status.",
                DailyPopulationType.ADMINISTRATIVE: "The number of people who are supervised by the agency on administrative status.",
                DailyPopulationType.ABSCONDED: "The number of people who are supervised by the agency on absconscion status.",
                DailyPopulationType.HOLD_OR_SANCTION: "The number of people supervised by the agency who are temporarily incarcerated or confined but are still considered to be on the supervision caseload.",
                DailyPopulationType.OTHER: "The number of people who are supervised by the agency in the community and have another supervision status that is not active, administrative, absconder, or incarcerated on a hold or sanction.",
                DailyPopulationType.UNKNOWN: "The number of people who are supervised by the agency in the community and have an unknown supervision status.",
            },
        ),
        AggregatedDimension(dimension=RaceAndEthnicity, required=True),  # TODO(#17579)
        AggregatedDimension(
            # TODO(#17579)
            dimension=BiologicalSex,
            required=True,
            dimension_to_description={
                BiologicalSex.MALE: "The number of people who are supervised under the jurisdiction of the agency whose biological sex is male.",
                BiologicalSex.FEMALE: "The number of people who are supervised under the jurisdiction of the agency whose biological sex is female.",
                BiologicalSex.UNKNOWN: "The number of people who are supervised under the jurisdiction of the agency whose biological sex is not known.",
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

discharges = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.SUPERVISION_TERMINATIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Discharges",
    description="The number of people who had a supervision term that ended. In some instances, this may mean being released from the jurisdiction of the supervision agency. In others, it may mean transitioning from one term of supervision to another or that a supervision term ended due to revocation to incarceration.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=SupervisionDischargesIncludesExcludes,
        excluded_set={
            SupervisionDischargesIncludesExcludes.TRANSFERRED,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=DischargeType,
            required=True,
            dimension_to_includes_excludes={
                DischargeType.SUCCESSFUL: IncludesExcludesSet(
                    members=SupervisionSuccessfulCompletionIncludesExcludes,
                    excluded_set={
                        SupervisionSuccessfulCompletionIncludesExcludes.OUTSTANDING_VIOLATIONS,
                        SupervisionSuccessfulCompletionIncludesExcludes.ABSCONSCION,
                        SupervisionSuccessfulCompletionIncludesExcludes.DIED,
                        SupervisionSuccessfulCompletionIncludesExcludes.INCARCERATION,
                        SupervisionSuccessfulCompletionIncludesExcludes.REVOKED,
                        SupervisionSuccessfulCompletionIncludesExcludes.TERMINATED,
                    },
                ),
                DischargeType.NEUTRAL: IncludesExcludesSet(
                    members=SupervisionNeutralDischargeIncludesExcludes,
                    excluded_set={
                        SupervisionNeutralDischargeIncludesExcludes.INCARCERATION,
                        SupervisionNeutralDischargeIncludesExcludes.REVOKED,
                        SupervisionNeutralDischargeIncludesExcludes.TERMINATED,
                        SupervisionNeutralDischargeIncludesExcludes.COMPLETED_REQUIREMENTS,
                        SupervisionNeutralDischargeIncludesExcludes.EARLY_RELEASE,
                        SupervisionNeutralDischargeIncludesExcludes.END_OF_TERM,
                    },
                ),
                DischargeType.UNSUCCESSFUL: IncludesExcludesSet(
                    members=SupervisionUnsuccessfulDischargeIncludesExcludes,
                    excluded_set={
                        SupervisionUnsuccessfulDischargeIncludesExcludes.OUTSTANDING_VIOLATIONS,
                        SupervisionUnsuccessfulDischargeIncludesExcludes.ABSCONSCION,
                        SupervisionUnsuccessfulDischargeIncludesExcludes.DIED,
                        SupervisionUnsuccessfulDischargeIncludesExcludes.COMPLETED_REQUIREMENTS,
                        SupervisionUnsuccessfulDischargeIncludesExcludes.EARLY_RELEASE,
                        SupervisionUnsuccessfulDischargeIncludesExcludes.END_OF_TERM,
                    },
                ),
            },
            dimension_to_description={
                DischargeType.SUCCESSFUL: "The number of people who had a term of supervision end due to successful completion of required terms or timeframe.",
                DischargeType.NEUTRAL: "The number of people who had a term of supervision end without a clear successful completion or failure event such as revocation.",
                DischargeType.UNSUCCESSFUL: "The number of people who had a term of supervision end due to unsatisfactory compliance.",
                DischargeType.OTHER: "The number of people who had a term of supervision end for reasons that are not considered successful completions, neutral discharges, or unsuccessful discharges.",
                DischargeType.UNKNOWN: "The number of people who had a supervision term end for unknown reasons.",
            },
        )
    ],
)

reconvictions = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.RECONVICTIONS,
    category=MetricCategory.PUBLIC_SAFETY,
    display_name="Reconvictions",
    description="The number of people convicted of a new crime while serving a term of supervision.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.ANNUAL],
    includes_excludes=IncludesExcludesSet(
        members=SupervisionReconvictionsIncludesExcludes,
        excluded_set={
            SupervisionReconvictionsIncludesExcludes.NEW_INFRACTION,
        },
    ),
)

caseload_numerator = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.CASELOADS_PEOPLE,
    category=MetricCategory.POPULATIONS,
    display_name="Open Cases",
    description="Number of people with open cases under the jurisdiction of the supervision agency (used as the numerator in the calculation of the caseloads metric).",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=SupervisionCaseloadNumeratorIncludesExcludes,
    ),
)

caseload_denominator = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.CASELOADS_STAFF,
    category=MetricCategory.CAPACITY_AND_COST,
    display_name="Staff with Caseload",
    description="The number of staff carrying a supervision caseload (used as the denominator in the calculation of the caseloads metric).",
    measurement_type=MeasurementType.INSTANT,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    includes_excludes=IncludesExcludesSet(
        members=SupervisionCaseloadDenominatorIncludesExcludes,
        excluded_set={
            SupervisionCaseloadDenominatorIncludesExcludes.STAFF_ON_LEAVE,
        },
    ),
)

revocations = MetricDefinition(
    system=System.SUPERVISION,
    metric_type=MetricType.REVOCATIONS,
    category=MetricCategory.POPULATIONS,
    display_name="Revocations",
    description="The number of people who had a term of supervision revoked.",
    measurement_type=MeasurementType.DELTA,
    reporting_frequencies=[ReportingFrequency.MONTHLY],
    # TODO(#17579)
    includes_excludes=IncludesExcludesSet(
        members=SupervisionRevocationsIncludesExcludes,
        excluded_set={
            SupervisionRevocationsIncludesExcludes.TERMINATION,
            SupervisionRevocationsIncludesExcludes.SHORT_TERM_INCARCERATION,
        },
    ),
    aggregated_dimensions=[
        AggregatedDimension(
            dimension=RevocationType,
            required=True,
            dimension_to_includes_excludes={
                ViolationType.TECHNICAL: IncludesExcludesSet(
                    members=SupervisionTechnicalViolationsIncludesExcludes,
                    excluded_set={
                        SupervisionTechnicalViolationsIncludesExcludes.CRIMINAL_OFFENSE,
                        SupervisionTechnicalViolationsIncludesExcludes.ARREST,
                        SupervisionTechnicalViolationsIncludesExcludes.CONVICTION,
                        SupervisionTechnicalViolationsIncludesExcludes.ABSCONDING,
                    },
                ),
                ViolationType.NEW_OFFENSE: IncludesExcludesSet(
                    members=SupervisionNewOffenseViolationsIncludesExcludes,
                ),
            },
            dimension_to_description={
                RevocationType.TECHNICAL: "The number of people revoked from supervision whose most serious violation was defined as technical.",
                RevocationType.NEW_OFFENSE: "The number of people revoked from supervision whose most serious violation was defined as a new offense.",
                RevocationType.OTHER: "The number of people revoked from supervision who were revoked for a violation that was neither technical nor a new offense.",
                RevocationType.UNKNOWN: "The number of people revoked from supervision for an unknown reason.",
            },
        ),
    ],
)
