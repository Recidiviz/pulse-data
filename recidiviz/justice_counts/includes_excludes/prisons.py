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


# Population
class PopulationIncludesExcludes(Enum):
    STATE_PRISON = "People in state prison, penitentiary, or correctional institutions"
    BOOT_CAMPS = "People in boot camps"
    RECEPTION_CENTERS = "People in reception, diagnostic, and classification centers"
    RELEASE_CENTERS = "People in release centers"
    TREATMENT_FACILITIES = (
        "People in drug and alcohol treatment facilities for people in prison"
    )
    HALFWAY_HOUSE = "People in prison-run halfway houses and transitional housing"
    LOCAL_JAIL = "People in the prison agency’s jurisdiction and held in local jail"
    PRIVATE_PRISON = "People in the prison agency’s jurisdiction and held in private prison facilities"
    OUT_OF_STATE = "People in the prison agency’s jurisdiction and held in other states’ prison facilities"
    TEMPORARY_ABSENT = "People who are temporarily absent for less than 30 days (e.g., furlough, hospital, work release)"
    NOT_CONVICTED = "People who have not been convicted of committing crime(s) but are being held by the agency"


# Funding
class PrisonFundingTimeframeIncludesExcludes(Enum):
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM_FUNDING = "Biennium funding appropriated during the time period"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that are appropriated in during the time period"
    )


class PrisonFundingPurposeIncludesExcludes(Enum):
    FACILITY_MAINTENANCE = "Funding for prison facility operations and maintenance"
    OPERATIONS = "Funding for operations and maintenance of other facilities within the agency’s jurisdiction (e.g., transitional housing facilities, treatment facilities, etc.)"
    FACILITY_CONSTRUCTION = (
        "Funding for construction or rental of new prison facilities"
    )
    TREATMENTS_IN_FACILITIES = (
        "Funding for agency-run or contracted treatment and programming"
    )
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
class PrisonExpensesTimeframeAndSpendDownIncludesExcludes(Enum):
    SINGLE_YEAR = "Expenses for single fiscal year"
    BIENNNIUM_EXPENSES = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )


class PrisonExpensesTypeIncludesExcludes(Enum):
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
    TRAINING_ACADEMY = "Training academy"
    SPECIALIZED = "Specialized training"
    EXTERNAL = "External training or professional development opportunities (e.g., conferences, classes, etc.)"
    NO_COST = (
        "Courses or programs offered at no cost to individuals or the court system"
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
    VACANT = "Clinical and medical staff positions budgeted but currently vacant"


class PrisonProgrammaticStaffIncludesExcludes(Enum):
    VOCATIONAL = "Vocational staff"
    EDUCATION = "Educational staff"
    THERAPEUTIC = "Therapeutic and support program staff"
    RELIGIOUS = "Religious or cultural program staff"
    RESTORATIVE_JUSTICE = "Restorative Justice staff"
    VOLUNTEER = "Programmatic staff volunteer positions"
    VACANT = "Programmatic staff positions budgeted but currently vacant"


class VacantPrisonStaffIncludesExcludes(Enum):
    SUPERVISION = "Vacant security staff positions"
    MANAGEMENT_OPERATIONS = "Vacant management and operations staff positions"
    CLINICAL_OR_MEDICAL = "Vacant clinical and medical staff positions"
    PROGRAMMATIC = "Vacant programmatic staff positions"
    UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"


# Admissions
class PrisonAdmissionsIncludesExcludes(Enum):
    """Includes/Excludes for Prisons Admissions at the metric-level."""

    NEW_SENTENCES = "Admissions to prison for a new prison sentence"
    BOARD_HOLD = "Admissions to prison for a parole board hold"
    SUSPENDED_SENTENCE = (
        "Admissions to prison to serve a suspended sentence of prison incarceration"
    )
    SPLIT_SENTENCE = (
        "Admissions to prison to serve a split sentence of prison incarceration"
    )
    SHOCK_PROBATION = "Admissions to prison to serve a shock probation sentence"
    PROBATION_SANCTION = (
        "Admissions to prison to serve a probation supervision incarceration sanction"
    )
    REVOCATION_POST_ADJUCATION_SUPERVISION = "Admissions to prison due to a revocation of post-adjudication community supervision sentence (i.e., probation, parole, or other community supervision sentence type)"
    IMPOSED_SUPERVISION_AGENCY = "Admissions to prison due to a post-adjudication incarceration sanction imposed by a community supervision agency (e.g., a “dip,” “dunk,” or weekend sentence)"
    IMPOSED_COURT = "Admissions to prison due to a post-adjudication incarceration sanction imposed by a specialty, treatment, or problem-solving court (e.g., a “dip,” “dunk,” or weekend sentence)"
    REVOKED_PAROLE = "Admissions to prison for revocation of parole supervision"
    AWOL_STATUS = (
        "Admissions to prison from Absent Without Leave (AWOL) status or escape status"
    )
    UNDER_JURISDICTION = "Admissions to prison from jail(s) that are under the jurisdiction of the agency"
    OUTSIDE_JURISDICTION = "Admissions to prison from jail(s) that are outside of the jurisdiction of the agency"
    PRIVATE_PRISON = (
        "Admissions to private prison facilities contracted with the agency"
    )
    INTERSTATE_TRANSFER = "Admissions to prison from prison facilities in other states that are not under the jurisdiction of the agency (e.g., interstate transfer)"
    TEMPORARY_ABSENCES = "Admissions to prison from temporary absences (e.g., from court, hospital, furlough, work release)"
    TRANSER_SAME_STATE = "Admissions to prison due to transfers between prison facilities in the same state"
    FEDERAL_HOLD_US_MARSHALS_SERVICE = "Admissions to prison due to a pre-adjudication federal hold for U.S. Marshals Service, Federal Bureau of Prisons, or U.S. Immigration and Customs Enforcement"
    FEDERAL_HOLD_TRIBAL = "Admissions to prison due to a pre-adjudication federal hold for a Tribal Nation or the Bureau of Indian Affairs"
    AWAITING_HEARINGS = "Admissions to prison for people awaiting hearings for failure to appear in court or court-ordered programs"
    FAILURE_TO_PAY = "Admissions to prison due to failure to pay fines or fees ordered by civil or criminal courts"


# Releases
class PrisonReleasesIncludesExcludes(Enum):
    AFTER_PRISON = "People released to the community after completing a prison incarceration sentence"
    AFTER_PAROLE_DECISION = (
        "People released to the community after the decision of a parole board"
    )
    TRANSFERRED_NOT_WITHIN = (
        "People transferred to a prison in another jurisdiction (state or federal)"
    )
    DIED = "People who died while under the agency’s jurisdiction"
    ESCAPED = "People who escaped or who are AWOL from the agency’s jurisdiction"
    TRANSFERRED_WITHIN = (
        "People who transferred to another facility within the agency’s jurisdiction"
    )
    TEMPORARILY_TRANSFERRED = "People who are temporarily transferred out of the agency’s facilities (to court, hospital, etc.)"
    TEMPORARILY_ABSENT = "People who are temporarily absent from the agency’s facilities for less than 30 days"


class PrisonReleasesToProbationIncludesExcludes(Enum):
    COMPLETED_SENTENCE = "Releases to an additional probation sentence after completing a prison sentence"
    AFTER_SANCTION = (
        "Releases back to probation after a prison incarceration probation sanction"
    )
    SPLIT_SENTENCE = "Releases to probation to serve a split or on-and-after sentence"
    SHOCK_PROBATION = "Releases to probation after a shock probation sentence"
    TRANSFERRED_OUT = "Releases to probation in the jurisdiction of another state"


class PrisonReleasesToParoleIncludesExcludes(Enum):
    AFTER_SANCTION = (
        "Releases back to parole after a prison incarceration parole sanction"
    )
    ELIGIBLE = "Releases to parole at eligibility for release from prison"
    COMMUTED_SENTENCE = "Releases to parole due to commuted or lowered sentence"
    RELEASE_TO_PAROLE = (
        "Releases to parole or re-parole in the jurisdiction of another state"
    )


class PrisonReleasesCommunitySupervisionIncludesExcludes(Enum):
    RELEASED_TO_OTHER_AGENCY = "Releases to other community supervision agency"
    DUAL_SUPERVISION = "Releases to dual or concurrent supervision with more than one community supervision agency"


class PrisonReleasesNoControlIncludesExcludes(Enum):
    NO_POST_RELEASE = "Releases due to sentence completion, no post-release supervision"
    EXONERATION = "Releases due to exoneration after conviction"


class PrisonReleasesDeathIncludesExcludes(Enum):
    DEATH = "Releases due to death of people in custody"
    DEATH_WHILE_ABSENT = "Releases due to death of people in custody who were temporarily absent (e.g., hospital, court, work release)"


# Readmissions
class PrisonReadmissionsNewConvictionIncludesExcludes(Enum):
    NEW_SENTENCES = "People with new prison sentences"
    SPLIT_SENTENCE = "People with new split sentences"
    REVOKED_NEW_OFFENSE = "People revoked from any form of supervision due to a new criminal offense that do not have a new criminal conviction"
    REVOKED_NEW_CONVICTION = (
        "People revoked from any form of supervision due to a new criminal conviction"
    )


class PrisonReadmissionsProbationIncludesExcludes(Enum):
    PENDING_HEARING = "People on a probation hold pending a hearing"
    SERVING_SANCTION = "People on probation serving an incarceration sanction"
    TECHNICAL_VIOLATION = "People revoked from probation for technical violation(s)"
    NEW_OFFENSE = "People revoked from probation for a new criminal offense that do not have a new criminal conviction"
    NEW_CONVICTION = "People revoked from probation for a new criminal conviction"


class PrisonReadmissionsParoleIncludesExcludes(Enum):
    PENDING_HEARING = "People on a parole hold pending a hearing"
    SERVING_SANCTION = "People on parole serving an incarceration sanction"
    TECHNICAL_VIOLATION = "People revoked from parole for technical violation(s)"
    NEW_OFFENSE = "People revoked from parole due to a new criminal offense that do not have a new criminal conviction"
    NEW_CONVICTION = "People revoked from parole due to a new criminal conviction"
    TRANSFERRED = "People transferred from parole in another jurisdiction"


class PrisonReadmissionsOtherCommunitySupervisionIncludesExcludes(Enum):
    PENDING_HEARING = (
        "People on a hold pending a hearing for another community supervision type"
    )
    SERVING_SANCTION = (
        "People on other community supervision serving an incarceration sanction"
    )
    TECHNICAL_VIOLATION = (
        "People revoked from other community supervision for technical violation(s)"
    )
    NEW_OFFENSE = "People revoked from other community supervision for a new criminal offense that do not have a new criminal conviction"
    NEW_CONVICTION = (
        "People revoked from other community supervision for a new criminal conviction"
    )


# Use of force incidents
class PrisonUseOfForceIncludesExcludes(Enum):
    PHYSICAL_FORCE = "Incidents involving physical force"
    RESTRAINING_DEVICES = "Incidents involving the use of restraining devices (e.g., handcuffs, leg irons)"
    USE_OF_WEAPONS = "Incidents involving the use of weapons"
    OTHER_TYPES = "Incidents involving the use of other types of force"
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
    FILED_BY_VISITOR = "Grievances filed by other people who are not incarcerated in the agency’s facilities (e.g., visitors)"
    FILED_BY_STAFF = "Grievances filed by staff employed by the agency"


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
    MEDICAL_STAFF = "Grievances against clinical and medical staff"
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
