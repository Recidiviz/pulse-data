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
"""Includes/Excludes definition for prison agencies """

from enum import Enum


# Funding
class PrisonFundingIncludesExcludes(Enum):
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that will not be fully spent this fiscal year"
    )
    FACILITY_MAINTENANCE = "Funding for prison facility operations and maintenance"
    OPERATIONS = "Funding for operations and maintenance of other facilities within the agency’s jurisdiction (e.g., transitional housing facilities, treatment facilities, etc.)"
    FACILITY_CONSTRUCTION = "Funding for construction of new prison facilities"
    TREATMENTS_IN_FACILITIES = "Funding for agency run or contracted treatment and programming within facilities"
    HEALTH_CARE = "Funding for health care for people in prison facilities"
    FACILITY_STAFF = "Funding for prison facility staff"
    ADMIN_AND_SUPPORT_STAFF = "Funding for central administrative and support staff"
    CONTRACTED_BEDS = (
        "Funding for the operation of private prison beds contracted by the agency"
    )
    JAIL_OPERATIONS = "Funding for jail facility operations and maintenance"
    JUVENILE_JAILS = "Funding for juvenile jail facilities"
    NON_PRISON_ACTIVITIES = "Funding for non-prison activities such as pre- or post-adjudication community supervision"
    LAW_ENFORCEMENT = "Funding for law enforcement functions"


class PrisonsFundingStateAppropriationIncludesExcludes(Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"
    GRANTS = "Grants from state sources that are not budget appropriations approved by the legislature/governor"


class PrisonsFundingGrantsIncludesExcludes(Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE = "Private or foundation grants"


class PrisonsFundingCommissaryAndFeesIncludesExcludes(Enum):
    SALES = "Sales in prison commissaries"
    CHARGES_INCARCERATED = "Fees charged to people who are incarcerated"
    CHARGES_VISITORS = "Fees charged to visitors of people who are incarcerated"


class PrisonsFundingContractBedsIncludesExcludes(Enum):
    COUNTY = "Funding collected from beds contracted by county agencies"
    STATE = "Funding collected from beds contracted by state agencies"
    FEDERAL = "Funding collected from beds contracted by federal agencies"


# Expenses
class PrisonExpensesIncludesExcludes(Enum):
    SINGLE_YEAR = "Expenses for single fiscal year"
    BIENNNIUM_EXPENSES = "Biennium expenses"
    MULTI_YEAR = "Multi-year expenses that will not be fully spent this fiscal year"
    PRISON_FACILITY = "Expenses for prison facility operations and maintenance"
    OPERATIONS_AND_MAINTENANCE = "Expenses for operations and maintenance of other facilities within the agency’s jurisdiction (e.g., transitional housing facilities, treatment facilities, etc.)"
    CONSTRUCTION = "Expenses for construction or rental of new prison facilities"
    TREATMENT = "Expenses for agency-run or contracted treatment and programming"
    HEALTH_CARE = "Expenses for health care for people in prison facilities"
    FACILITY_STAFF = "Expenses for prison facility staff"
    ADMINISTRATION_STAFF = "Expenses for central administrative and support staff"
    OPERATION = (
        "Expenses for the operation of private prison beds contracted by the agency"
    )
    JAIL_FACILITY = "Expenses for jail facility operations and maintenance"
    JUVENILE_JAIL = "Expenses for juvenile jail facilities"
    NON_PRISON_ACTIVITIES = "Expenses for non-prison activities such as pre- or post-adjudication community supervision"
    LAW_ENFORCEMENT = "Expenses for law enforcement functions"


class PrisonExpensesPersonnelIncludesExcludes(Enum):
    SALARIES = "Salaries"
    BENEFITS = "Benefits"
    RETIREMENT = "Retirement contributions"
    CONTRACTORS = "Costs for individuals contracted to work for the prison agency"
    COMPANIES_AND_SERVICES = "Costs for companies or service providers contracted to support work with prison agencies"


class PrisonExpensesTrainingIncludesExcludes(Enum):
    ANNUAL_TRAINING = "Annual training"
    CONTINUING_EDUCATION = "Continuing education"
    TRAINING_ACADEMY = "Training academy"
    EXTERNAL_TRAINING = "External training or professional development opportunities (e.g., conferences, classes, etc.)"
    FREE_PROGRAMS = (
        "Courses or programs offered at no cost to individuals or the department"
    )


class PrisonExpensesFacilitiesAndEquipmentIncludesExcludes(Enum):
    OPERATIONS = "Prison facility operations"
    MAINTENANCE = "Prison facility maintenance"
    RENOVATION = "Prison facility renovation"
    CONSTRUCTION = "Prison facility construction"
    EQUIPMENT = "Equipment (e.g., computers, communication, and information technology infrastructure"


class PrisonExpensesHealthCareIncludesExcludes(Enum):
    OPERATIONS = (
        "Expenses related to the operation of prison facility infirmaries and hospitals"
    )
    SALARIES = (
        "Salaries and benefits for medical providers employed by the prison agency"
    )
    CONTRACTS = "Contracts with providers of medical care"
    PHYSICAL_MEDICAL_CARE = "Expenses related to physical medical care"
    MENTAL_HEALTH_CARE = "Expenses related to mental health care"
    TRANSPORTATION = "Costs related to transporting people who are incarcerated to and from hospitals or other health care facilities"


class PrisonExpensesContractBedsIncludesExcludes(Enum):
    JAILS = "Expenses for beds contracted to county jail agencies"
    OUT_OF_STATE_PRISONS = (
        "Expenses for beds contracted to other states’ prisons agencies"
    )
    FEDERAL_AGENCIES = "Expenses for beds contracted by federal agencies"
    PRIVATE_PRISONS = "Expenses for beds contracted by private prison companies"


# Total staff
class PrisonStaffIncludesExcludes(Enum):
    FILLED = "Filled positions"
    VACANT = "Staff positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions"
    PART_TIME = "Part-time positions"
    CONTRACTED = "Contracted positions"
    TEMPORARY = "Temporary positions"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"


class PrisonSecurityStaffIncludesExcludes(Enum):
    CORRECTIONAL_OFFICERS = "Correctional officers (all ranks)"
    SUPERVISORS = "Correctional officer supervisors"
    VACANT = "Security staff positions budgeted but currently vacant"


class PrisonManagementAndOperationsStaffIncludesExcludes(Enum):
    MANAGEMENT = "Prison management (i.e., executive level staff such as the warden, chiefs, superintendents, etc.)"
    CLERICAL_OR_ADMIN = "Clerical and administrative staff"
    RESEARCH = "Research staff"
    MAINTENANCE = "Maintenance staff"
    VACANT = "Management and operations staff positions budgeted but currently vacant"


class PrisonClinicalStaffIncludesExcludes(Enum):
    DOCTORS = "Medical doctors"
    NURSES = "Nurses"
    DENTISTS = "Dentists"
    CLINICIANS = "Clinicians (e.g., substance use treatment specialists)"
    THERAPISTS = "Therapists (e.g., mental health counselors)"
    PSYCHIATRISTS = "Psychiatrists"
    VACANT = "Clinical or medical staff positions budgeted but currently vacant"


class PrisonProgrammaticStaffIncludesExcludes(Enum):
    VOCATIONAL = "Vocational staff"
    EDUCATION = "Educational staff"
    THERAPEUTIC = "Therapeutic and support program staff"
    RELIGIOUS = "Religious or cultural program staff"
    RESTORATIVE_JUSTICE = "Restorative Justice staff"
    VACANT = "Programmatic staff positions budgeted but currently vacant"
    VOLUNTEER = "Programmatic staff volunteer positions"


class VacantPrisonStaffIncludesExcludes(Enum):
    SUPERVISION = "Vacant supervision staff positions"
    NON_SUPERVISION = "Vacant non-supervision administrative staff positions"
    CLINICAL_OR_MEDICAL = "Vacant clinical or medical staff positions"
    PROGRAMMATIC = "Vacant programmatic staff positions"
    FILLED = "Filled positions"


# Admissions
class PrisonAdmissionsIncludesExcludes(Enum):
    NEW_SENTENCES = "People with new prison sentences"
    BOARD_HOLD = "People entering prison on a board hold"
    SPLIT_SENTENCE = "People entering prison on a split sentence"
    SHOCK_PROBATION = "People entering prison on shock probation"
    INCARCERATION_SANCTION = (
        "People entering prison to serve an incarceration sanction while on probation"
    )
    REVOKED_PROBATION = "People entering prison who were revoked from probation"
    REVOKED_PAROLE = "People entering prison who were revoked from parole"
    AWOL_STATUS = "People returning to prison from AWOL status or escape"
    TRANSFERRED_STATE = "People transferred from another state"
    TRANSFERRED_JAILS = (
        "People entering jails who are under the jurisdiction of the agency"
    )
    TRANSFERRED_PRIVATE_PRISONS = "People entering private prison facilities who are under the jurisdiction of the agency"
    TRANSFERRED_PRISONS = (
        "People entering prison facilities who are under the jurisdiction of the agency"
    )
    OTHER = "Other admissions not captured by the listed categories"
    RETURNING_FROM_ABSENCE = "People returning to prison from temporary absences (e.g., from court, hospital, furlough, work release)"
    TRANSFERRED_BETWEEN_FACILITIES = (
        "People transferred between facilities in the same state"
    )


class PrisonPersonOffenseIncludesExcludes(Enum):
    HOMICIDE = "Homicide or manslaughter"
    AGGRAVATED_ASSAULT = "Aggravated assault"
    SIMPLE_ASSAULT = "Simple assault"
    INTIMIDATION = "Intimidation"
    KIDNAPPING = "Kidnapping or abduction"
    ROBBERY = "Robbery"
    FORCIBLE_SEX_OFFENSES = "Forcible sex offenses"
    NON_FORCIBLE_SEX_OFFENSES = "Non-forcible sex offenses"
    HUMAN_TRAFFICKING = "Human trafficking"
    CHILD_PORNOGRAPHY = "Child Pornography"
    CHILD_ENDANGERMENT = "Child Endangerment"
    WEAPONS = "Weapons"
    HARASSMENT = "Harassment"
    ARSON = "Arson"
    OTHER = "Other person offenses not captured by the listed categories"


class PrisonPropertyOffenseIncludesExcludes(Enum):
    ARSON = "Arson"
    BURGLARY = "Burglary"
    COUNTERFEITING = "Counterfeiting or forgery"
    VANDALISM = "Destruction, damage, or vandalism of property"
    EMBEZZLEMENT = "Embezzlement"
    EXTORTION = "Extortion or blackmail"
    BAD_CHECKS = "Bad checks"
    FRAUD = "Fraud"
    LARCENY = "Larceny or theft"
    MOTOR_VEHICLE_THEFT = "Motor vehicle theft"
    STOLEN_PROPERTY = "Stolen property"
    OTHER = "Other property offenses not captured by the listed categories"


class PrisonDrugOffenseIncludesExcludes(Enum):
    PRODUCTION = "Production"
    TRANSPORTATION = "Transportation or importation"
    DISTRIBUTION = "Distribution, sale, or trafficking"
    POSSESSION = "Possession"
    PURCHASE = "Purchase"
    USE = "Use"
    EQUIPMENT_OR_DEVICES = "Equipment or devices"
    OTHER = "Other drug offenses not captured by the listed categories"


class PrisonPublicOrderOffenseIncludesExcludes(Enum):
    GAMBLING = "Gambling"
    BRIBERY = "Bribery"
    ADULT_PORNOGRAPHY = "Adult pornography/obscene material"
    PROSTITUTION = "Prostitution"
    CURFEW = "Curfew, loitering, or vagrancy"
    DUI = "Driving under the influence"
    DRUNKENNESS = "Drunkenness"
    TRESPASS = "Trespass"
    DISORDERLY_CONDUCT = "Disorderly conduct"
    HARASSMENT = "Harassment"
    VIOLATION_OR_RESTRAINING_ORDER = "Violation of a restraining order"
    OTHER_SEX_OFFENSES = "Other sex offenses not captured by the listed categories"
    NONVIOLENT_FAMILY_OFFENSES = "Nonviolent family offenses"
    VOYEURISM = "Voyeurism"
    OTHER = "Other public order offenses not captured by the listed categories"


# Average daily population


class PrisonAverageDailyPopulationIncludesExcludes(Enum):
    CORRECTIONAL_INSTITUTIONS = "People held in the agency’s correctional institutions"
    AGENCY_CAMPS = (
        "People held in the agency’s boot camps, conservation camps, and forestry camps"
    )
    AGENCY_RECEPTION_CENTERS = (
        "People held in the agency’s reception, diagnostic, and classification centers"
    )
    AGENCY_RELEASE_CENTERS = (
        "People held in the agency’s release centers, halfway houses, and road camps"
    )
    AGENCY_HOSPITALS = "People held in the agency’s hospitals and drug and alcohol treatment facilities"
    VOCATIONAL_FACILITIES = "People held in the agency’s vocational training facilities"
    TEMPORARILY_ABSENT = "People who are temporarily absent for less than 30 days (e.g., furlough, hospital, work release)"
    JAILS = "People under the agency’s jurisdiction held in local jails"
    PRIVATE_FACILITIES = (
        "People under the agency’s jurisdiction held in private facilities"
    )
    OUTSIDE_FACILITY = (
        "People under the agency’s jurisdiction held in another state’s facility"
    )
    OTHER = "Other people under the agency’s jurisdiction not captured by the listed categories"
    HOUSED_FOR_OTHER_AGENCIES = (
        "People housed in the agency’s correctional facilities for other jurisdictions"
    )
    AWOL = "People who are AWOL or have escaped (more than 30 days)"


# Releases
class PrisonReleasesIncludesExcludes(Enum):
    NO_CONTROL = "People released with no further correctional control"
    PAROLE = "People released to parole"
    PROBATION = "People released to probation"
    TRANSFERRED_OUT = "People transferred to another jurisdiction"
    DEATH = "People who died under the agency’s jurisdiction"
    AWOL = "People who escaped from the agency’s jurisdiction or are AWOL"
    COMMUTED_SENTENCE = "People whose sentence was commuted or lowered to time served"
    TEMPORARILY_ABSENT = "People who are temporarily absent for less than 30 days (e.g., furlough, hospital, work release)"
    EMERGENCY_RELEASE = "Emergency releases (such as during COVID-19)"
    JAIL_RELEASE = (
        "People released from jails who were under the jurisdiction of the agency"
    )
    PRIVATE_FACILITIES_RELEASE = "People released from private prison facilities who were under the jurisdiction of the agency"
    OTHER = "Other release not captured by the listed categories"
    TRANSFERRED_IN = "People transferred from one of the agency’s facilities to another"
    TEMP_TRANSFER = (
        "People who are temporarily transferred (e.g., to court or hospital)"
    )
    TEMP_EXIT = "People who exit temporarily (e.g., to work release or other)"


class PrisonReleasesToParoleIncludesExcludes(Enum):
    AUTOMATIC_OR_PRESUMPTIVE = (
        "People released on automatic or presumptive parole or re-parole"
    )
    PAROLE_BOARD_VOTE = (
        "People released to parole or re-parole after a positive parole board vote"
    )
    AFTER_SANCTION = "People returning to parole after a sanction served in prison"
    POST_RELEASE_SUPERVISION = "People released to another form of post-release supervision that is not probation (e.g., conditional release, provisional release)"
    COMMUTED_SENTENCE = "People whose sentence was commuted or lowered to time served and released to parole"
    TRANSFERRED_OUT = (
        "People released to parole or re-parole in the jurisdiction of another state"
    )
    OTHER = "Other releases to parole not captured by the listed categories"


class PrisonReleasesToProbationIncludesExcludes(Enum):
    COMPLETED_SENTENCE = "People released after completing a prison sentence who have an additional sentence of probation"
    AFTER_SANCTION = "People returning to probation after a sanction served in prison"
    SPLIT_SENTENCE = (
        "People released to probation to serve a split or on-and-after sentence"
    )
    SHOCK_PROBATION = "People released to probation after a shock probation sentence"
    COMMUTED_SENTENCE = "People whose sentence was commuted or lowered to time served and released to parole"
    TRANSFERRED_OUT = (
        "People released to probation in the jurisdiction of another state"
    )
    OTHER = "Other releases to probation not captured by the listed categories"


class PrisonReleasesNoAdditionalCorrectionalControlIncludesExcludes(Enum):
    NOT_ELIGIBLE_FOR_PAROLE = "People who were not eligible for parole and had no additional sentence to serve"
    APPROVED_FOR_PAROLE = "People who were approved for parole but maxed out with no additional sentence to serve"
    DENIED_PAROLE = "People who were denied parole and maxed out with no additional sentence to serve"
    COMMUTED_SENTENCE = "People whose sentence was commuted or lowered to time served with no additional supervision"
    OTHER = "Other releases to probation not captured by the listed categories"


class PrisonReleasesDeathIncludesExcludes(Enum):
    DEATH = "People who died while under the agency’s jurisdiction"
    DEATH_WHILE_ABSENT = (
        "People who died while temporarily absent (e.g., hospital, court, work release)"
    )
    OTHER = "Other deaths not captured by the listed categories"


# Readmissions
class PrisonReadmissionsIncludesExcludes(Enum):
    NEW_CONVICTION = "Admission due to a new criminal court conviction resulting in a prison sentence"
    RETURN_FROM_PROBATION = "Admission due to a return from probation"
    RETURN_FROM_PAROLE = "Admission due to a return from parole"
    RETURN_FROM_SUPERVISION = (
        "Admission due to a return from other community supervision"
    )


class PrisonReadmissionsNewConvictionIncludesExcludes(Enum):
    NEW_SENTENCES = "People with new prison sentences"
    SPLIT_SENTENCE = "People with new split sentences"


class PrisonReadmissionsProbationIncludesExcludes(Enum):
    PROBATION = "Admissions to serve a probation supervision incarceration sanction"
    PROBATION_REVOCATION = "Admissions due to revocation from probation supervision for technical violation(s)"
    NEW_VIOLATION = "Admissions due to revocations from probation supervision for new offense violation(s)"
    TRANSFERRED_IN = "Admissions due to transfers from another jurisdiction of people who were on probation supervision immediately prior to admission"
    PENDING_HEARING = "Admissions for holds pending a probation hearing decision for people on probation supervision immediately prior to their admission"


class PrisonReadmissionsParoleIncludesExcludes(Enum):
    PAROLE = "Admissions to serve a parole supervision incarceration sanction"
    TECHNICAL_VIOLATION = (
        "Admissions due to revocations from parole for technical violation(s)"
    )
    NEW_VIOLATION = (
        "Admissions due to revocations from parole for new offense violation(s)"
    )
    TRANSFERRED_IN = "Admissions due to transfers from another jurisdiction of people who were on parole supervision immediately prior to admission"
    PENDING_HEARING = "Admissions for holds pending a parole hearing decision for people on parole supervision immediately prior to their admission"
    BOARD_HOLD = "Admissions due to a parole board hold"


# Use of force incidents
class PrisonUseOfForceIncludesExcludes(Enum):
    PHYSICAL_FORCE = "Incidents involving physical force"
    RESTRAINING_DEVICES = "Incidents involving the use of restraining devices (e.g., handcuffs, leg irons)"
    USE_OF_WEAPONS = "Incidents involving the use of weapons"
    OTHER_TYPES = "Incidents involving uses of other types of force"
    JUSTIFIED = "Incidents found to be justified"
    UNJUSTIFIED = "Incidents not found to be justified"
    SPONTANEOUS_INCIDENTS = (
        "Incidents that are spontaneous (e.g., responses to emergent situations)"
    )
    PLANNED_INCIDENTS = (
        "Incidents that are planned (e.g., controlling a person for search or safety)"
    )
    ROUTINE = "Use of restraints during routine operations and movement of people in the agency’s jurisdiction that follows jurisdiction policy and standard operating procedures"


# Grievances upheld
class PrisonGrievancesIncludesExcludes(Enum):
    UPHELD = "Grievances upheld or substantiated"
    REMEDY = "Grievances resulting in a remedy (e.g., apology, policy change)"
    UNSUBSTANTIATED = "Grievances unsubstantiated"
    PENDING_RESOLUTION = "Grievances pending resolution"
    INFORMAL = "Grievances submitted informally or not in writing"
    DUPLICATE = "Duplicate grievances"


class PrisonGrievancesLivingConditionsIncludesExcludes(Enum):
    CLASSIFICATION = "Grievances related to classification of a person under the agency’s jurisdiction"
    ADMIN_SEGREGATION = "Grievances related to the use of administrative segregation"
    DISCIP_SEGREGATION = "Grievances related to the use of disciplinary segregation"
    OVERCROWDING = "Grievances related to overcrowding"
    UNSANITARY_CONDITIONS = "Grievances related to unsanitary conditions in the facility in general (i.e., not specific to living conditions)"
    FOOD = "Grievances related to food"
    MAINTENANCE = "Grievances related to facility maintenance issues"
    TESTING = "Grievances related to testing bodily fluids"
    SEARCHES = "Grievances related to body searches"
    PERSONAL_PROPERTY = "Grievances related to personal property"


class PrisonGrievancesPersonalSafetyIncludesExcludes(Enum):
    PHYSICAL_HARM_BY_STAFF = (
        "Grievances related to physical harm or threats of physical harm by staff"
    )
    PHYSICAL_HARM_BY_AGENCY = "Grievances related to physical harm or threats of physical harm by another person under the agency’s jurisdiction"
    EMOTIONAL_HARM_BY_STAFF = (
        "Grievances related to emotional harm or threats of emotional harm by staff"
    )
    EMOTIONAL_HARM_BY_AGENCY = "Grievances related to emotional harm or threats of emotional harm by another person under the agency’s jurisdiction"
    HARASSMENT_HARM_BY_STAFF = "Grievances related to harassment by staff"
    HARASSMENT_HARM_BY_AGENCY = "Grievances related to harassment by another person under the agency’s jurisdiction"
    PREA = "Grievances related to the Prison Rape Elimination Act (PREA)"


class PrisonGrievancesDiscriminationIncludesExcludes(Enum):
    RACIAL_DISCRIMINATION_STAFF = (
        "Grievances related to discrimination or racial bias by staff"
    )
    RACIAL_DISCRIMINATION_AGENCY = "Grievances related to discrimination or racial bias by another person under the agency’s jurisdiction"
    RELIGIOUS_DISCRIMINATION = "Grievances related to the ability of a person under the agency’s jurisdiction to practice or observe their religious beliefs"
    RELIGIOUS_DISCRIMINATION_LEVIED = "Grievances related to the ability of the person under the agency’s jurisdiction to practice or observe their religious beliefs levied at other people incarcerated in the agency’s jurisdiction"


class PrisonGrievancesHealthCareIncludesExcludes(Enum):
    DENIAL_OF_CARE = (
        "Grievances related to denial of care by medical or correctional personnel"
    )
    LACK_OF_TIMELY_CARE = "Grievances related to lack of timely health care"
    MEDICAL_STAFF = "Grievances against medical staff"
    REPRODUCTIVE_HEALTH_CARE = "Grievances related to reproductive health care"
    PRIVACY = "Grievances related to confidentiality or privacy issues"
    MEDICATION = "Grievances related to medication"
    MEDICAL_EQUIPMENT = "Grievances related to medical equipment"


class PrisonGrievancesLegalIncludesExcludes(Enum):
    LEGAL_FACILITIES = "Grievances related to access to legal facilities"
    LEGAL_MATERIALS = "Grievances related to access to legal materials"
    LEGAL_SERVICES = "Grievances related to access to legal services"
    LEGAL_PROPERTY = "Grievances related to access to legal property"
    LEGAL_COMMUNICATION = "Grievances related to access to legal communication"
