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
"""Includes/Excludes definition for jails agencies"""

from enum import Enum


# Funding
class FundingTimeframeIncludesExcludes(Enum):
    SINGLE_YEAR = "Funding for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )


class FundingPurposeIncludesExcludes(Enum):
    FACILITY_OPERATIONS = "Funding for jail facility operations and maintenance"
    OTHER_FACILITIES = "Funding for operations and maintenance of other facilities within the agency’s jurisdiction (e.g., transitional housing facilities, treatment facilities, etc.)"
    CONSTRUCTION = "Funding for construction or rental of new jail facilities"
    PROGRAMMING = "Funding for agency-run or contracted treatment and programming"
    HEALTH_CARE = "Funding for health care for people in jail facilities"
    FACILITY_STAFF = "Funding for jail facility staff"
    SUPPORT_STAFF = "Funding for central administrative and support staff"
    BEDS_CONTRACTED = (
        "Funding for the operation of private jail beds contracted by the agency"
    )
    CASE_MANAGEMENT_SYSTEMS = "Funding for electronic case management systems"
    OPERATIONS_MAINTENANCE = "Funding for prison facility operations and maintenance"
    JUVENILE = "Funding for juvenile jail facilities"
    NON_JAIL_ACTIVITIES = "Funding for non-jail activities such as pre- or post-adjudication community supervision"
    LAW_ENFORCEMENT = "Funding for law enforcement functions"


class CommissaryAndFeesIncludesExcludes(Enum):
    JAIL_COMMISSARIES = "Sales in jail commissaries"
    FEES_INCARCERATED = "Fees charged to people who are incarcerated"
    FEES_VISITORS = "Fees charged to visitors of people who are incarcerated"


class ContractBedsFundingIncludesExcludes(Enum):
    OTHER_COUNTY = "Funding collected from beds contracted by other county agencies"
    SUPERVISION = "Funding collected from beds contracted by supervision agencies"
    STATE = "Funding collected from beds contracted by state agencies"
    FEDERAL = "Funding collected from beds contracted by federal agencies"


# Expenses
class ExpensesTimeframeIncludesExcludes(Enum):
    SINGLE_YEAR = "Expenses for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )


class ExpensesTypeIncludesExcludes(Enum):
    FACILITY_OPERATIONS = "Expenses for jail facility operations and maintenance"
    OTHER_FACILITIES = "Expenses for operations and maintenance of other facilities within the agency’s jurisdiction (e.g., transitional housing facilities, treatment facilities, etc.)"
    CONSTRUCTION = "Expenses for construction or rental of new jail facilities"
    PROGRAMMING = "Expenses for agency-run or contracted treatment and programming"
    HEALTH_CARE = "Expenses for health care for people in jail facilities"
    FACILITY_STAFF = "Expenses for jail facility staff"
    SUPPORT_STAFF = "Expenses for central administrative and support staff"
    BEDS_CONTRACTED = (
        "Expenses for the operation of private jail beds contracted by the agency"
    )
    CASE_MANAGEMENT_SYSTEMS = "Expenses for electronic case management systems"
    OPERATIONS_MAINTENANCE = "Expenses for prison facility operations and maintenance"
    JUVENILE = "Expenses for juvenile jail facilities"
    NON_JAIL_ACTIVITIES = "Expenses for non-jail activities such as pre- or post-adjudication community supervision"
    LAW_ENFORCEMENT = "Expenses for law enforcement functions"


class PersonnelIncludesExcludes(Enum):
    SALARIES = "Salaries"
    BENEFITS = "Benefits"
    RETIREMENT = "Retirement contributions"
    INDIVIDUALS_CONTRACTED = (
        "Costs for individuals contracted to work in or for the jail agency"
    )
    COMPANIES_CONTRACTED = (
        "Costs for companies contracted to work in or for the jail agency"
    )


class TrainingIncludesExcludes(Enum):
    ANNUAL = "Annual training"
    CONTINUING = "Continuing education"
    ACADEMY = "Training academy"
    SPECIALIZED = "Specialized training"
    EXTERNAL = "External training or professional development opportunities (conferences, classes, etc.)"
    NO_COST_PROGRAMS = (
        "Courses or programs offered at no cost to individuals or the department"
    )


class FacilitiesAndEquipmentIncludesExcludes(Enum):
    OPERATIONS = "Jail facility operations"
    MAINTENANCE = "Jail facility maintenance"
    RENOVATION = "Jail facility renovation"
    CONSTRUCTION = "Jail facility construction"
    EQUIPMENT = "Equipment (e.g., computers, communication, and information technology infrastructure)"


class HealthCareForPeopleWhoAreIncarceratedIncludesExcludes(Enum):
    FACILITY_OPERATION = (
        "Expenses related to the operation of jail facility infirmaries and hospitals"
    )
    SALARIES_PROVIDERS = (
        "Salaries and benefits for medical providers employed by the jail agency"
    )
    CONTRACT_PROVIDERS = "Contracts with providers of medical care"
    EXPENSES_PHYSICAL = "Expenses related to physical medical care"
    EXPENSES_MENTAL = "Expenses related to mental health care"
    TRANSPORT_COSTS = "Costs related to transporting people who are incarcerated to and from hospitals or other health care facilities"


class ContractBedsExpensesIncludesExcludes(Enum):
    OTHER_AGENCIES = "Expenses for beds contracted with other jail agencies"
    STATE = "Expenses for beds contracted with state agencies (i.e., in state prisons)"
    FEDERAL = (
        "Expenses for beds contracted with federal agencies (i.e., in federal prisons)"
    )
    PRIVATE = "Expenses for beds contracted with private prison companies"


# Grievances Upheld
class GrievancesUpheldIncludesExcludes(Enum):
    UPHELD_OR_SUBSTANTIATED = "Grievances upheld or substantiated"
    REMEDY = "Grievances resulting in a remedy (e.g., apology, policy change)"
    UNSUBSTANTIATED = "Grievances unsubstantiated"
    PENDING_RESOLUTION = "Grievances pending resolution"
    INFORMAL = "Grievances submitted informally or not in accordance with the agency’s grievance policy"
    DUPLICATE = "Duplicate grievances"


class LivingConditionsIncludesExcludes(Enum):
    CLASSIFICATION = "Grievances related to classification of a person under the agency’s jurisdiction"
    ADMINISTRATIVE_SEGREGATION = (
        "Grievances related to the use of administrative segregation"
    )
    DISCIPLINARY_SEGREGATION = (
        "Grievances related to the use of disciplinary segregation"
    )
    OVERCROWDING = "Grievances related to overcrowding"
    UNSANITARY_CONDITIONS = "Grievances related to unsanitary conditions in the facility in general (i.e., not specific to living conditions)"
    FOOD = "Grievances related to food"
    FACILITY_MAINTENANCE = "Grievances related to facility maintenance issues"
    TESTING_BODILY_FLUIDS = "Grievances related to testing bodily fluids"
    BODY_SEARCHES = "Grievances related to body searches"
    PERSONAL_PROPERTY = "Grievances related to personal property"


class PersonalSafetyIncludesExcludes(Enum):
    PHYSICAL_HARM_STAFF = (
        "Grievances related to physical harm or threats of physical harm by staff"
    )
    PHYSICAL_HARM_ANOTHER_PERSON = "Grievances related to physical harm or threats of physical harm by another person under the agency’s jurisdiction"
    EMOTIONAL_HARM_STAFF = (
        "Grievances related to emotional harm or threats of emotional harm by staff"
    )
    EMOTIONAL_HARM_ANOTHER_PERSON = "Grievances related to emotional harm or threats of emotional harm by another person under the agency’s jurisdiction"
    HARASSMENT_STAFF = "Grievances related to harassment by staff"
    HARASSMENT_ANOTHER_PERSON = "Grievances related to harassment by another person under the agency’s jurisdiction"
    PREA = "Grievances related to the Prison Rape Elimination Act (PREA)"


class DiscriminationRacialBiasReligiousIncludesExcludes(Enum):
    DISCRIMINATION_STAFF = (
        "Grievances related to discrimination or racial bias by staff"
    )
    DISCRIMINATION_ANOTHER_PERSON = "Grievances related to discrimination or racial bias by another person incarcerated in the agency's jurisdiction"
    RELIGIOUS_BELIEFS_STAFF = "Grievances related to the ability of the person under the agency’s jurisdiction to practice or observe their religious beliefs levied at staff"
    RELIGIOUS_BELIEFS_ANOTHER_PERSON = "Grievances related to the ability of the person under the agency’s jurisdiction to practice or observe their religious beliefs levied at other people incarcerated in the agency’s jurisdiction"


class AccessToHealthCareIncludesExcludes(Enum):
    DENIAL_OF_CARE = (
        "Grievances related to denial of care by medical or correctional personnel"
    )
    LACK_OF_TIMELY_CARE = "Grievances related to lack of timely health care"
    MEDICAL_STAFF = "Grievances against medical staff"
    REPRODUCTIVE_CARE = "Grievances related to reproductive health care"
    GENDER_AFFIRMING_CARE = "Grievance related to access to gender affirming care"
    CONFIDENTIALITY = "Grievances related to confidentiality or privacy issues"
    MEDICATION = "Grievances related to medication"
    MEDICAL_EQUIPMENT = "Grievances related to medical equipment"


class LegalIncludesExcludes(Enum):
    FACILITIES = "Grievances related to access to legal facilities"
    MATERIALS = "Grievances related to access to legal materials"
    SERVICES = "Grievances related to access to legal services"
    PROPERTY = "Grievances related to access to legal property"
    COMMUNICATION = "Grievances related to access to legal communication"


# Staff
class StaffIncludesExcludes(Enum):
    FILLED = "Filled positions"
    VACANT = "Staff positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions"
    PART_TIME = "Part-time positions"
    CONTRACTED = "Contracted positions"
    TEMPORARY = "Temporary positions"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"


class SecurityStaffIncludesExcludes(Enum):
    CORRECTIONAL_OFFICERS_ALL = "Correctional officers (all ranks)"
    CORRECTIONAL_OFFICERS_SUPERVISORS = "Correctional officer supervisors"
    VACANT = "Security staff budgeted but currently vacant"


class ManagementAndOperationsStaffIncludesExcludes(Enum):
    JAIL_MANAGEMENT = "Jail management (i.e., executive-level staff such as the warden, chiefs, superintendent, etc.)"
    CLERICAL = "Clerical or administrative staff"
    RESEARCH = "Research staff"
    MAINTENANCE = "Maintenance staff"
    VACANT = "Management and operations staff positions budgeted but currently vacant"


class ClinicalAndMedicalStaffIncludesExcludes(Enum):
    DOCTORS = "Medical doctors"
    NURSES = "Nurses"
    DENTISTS = "Dentists"
    CLINICIANS = "Clinicians (e.g., substance use treatment specialists)"
    THERAPISTS = "Therapists (e.g., mental health counselors)"
    PSYCHIATRISTS = "Psychiatrists"
    VACANT = "Clinical or medical staff positions budgeted but currently vacant"


class ProgrammaticStaffIncludesExcludes(Enum):
    VOCATIONAL = "Vocational staff"
    EDUCATIONAL = "Educational staff"
    SUPPORT_PROGRAM = "Therapeutic and support program staff"
    RELIGIOUS = "Religious or cultural program staff"
    VACANT = "Programmatic staff positions budgeted but currently vacant"


class VacantPositionsIncludesExcludes(Enum):
    SECURITY = "Vacant security staff positions"
    MANAGEMENT_AND_OPERATIONS = "Vacant management and operations staff positions"
    CLINICAL_AND_MEDICAL = "Vacant clinical and medical staff positions"
    PROGRAMMATIC = "Vacant programmatic staff positions"
    UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"


# Use of Force Incidents
class UseOfForceIncidentsIncludesExcludes(Enum):
    PHYSICAL = "Incidents involving physical force"
    RESTRAINING_DEVICES = "Incidents involving the use of restraining devices (e.g., handcuffs, leg irons)"
    WEAPONS = "Incidents involving the use of weapons"
    OTHER_FORCE = "Incidents involving the use of other types of force"
    JUSTIFIED = "Incidents found to be justified"
    NOT_JUSTIFIED = "Incidents not found to be justified"
    SPONTANEOUS = (
        "Incidents that are spontaneous (e.g., responses to emergent situations)"
    )
    PLANNED = (
        "Incidents that are planned (e.g., controlling a person for search or safety)"
    )
    OTHER = "Other incidents not captured by the listed categories"
    ROUTINE = "Use of restraints during routine operations and movement of people in the agency’s jurisdiction that follows jurisdiction policy and standard operating procedures"


# Total Admissions
class TotalAdmissionsIncludesExcludes(Enum):
    """
    Enum representing various categories of individuals included or excluded in
    calculations of total jail admissions.
    """

    AWAITING_ARRAIGNMENT = "People in jail awaiting arraignment"
    UNPAID_BAIL = "People in jail due to unpaid bail"
    DENIAL_OF_BAIL = "People in jail due to denial of bail"
    REVOCATION_OF_BAIL = "People in jail due to revocation of bail"
    ASSESSMENT_OF_CAPACITY = (
        "People in jail pending assessment of capacity to stand trial"
    )
    TRANSFERRED_TO_HOSPITAL = "People who have been transferred to a hospital for a capacity assessment but are still counted on jail rolls"
    PENDING_PRETRIAL_REVOCATION = (
        "People in jail to be held pending outcome of pretrial revocation decision"
    )
    REVOCATION_OF_PRETRIAL_RELEASE = (
        "People in jail due to revocation of pretrial release"
    )
    PRETRIAL_SUPERVISION_SANCTION = (
        "People in jail due to a pretrial supervision incarceration sanction"
    )
    FEDERAL_HOLD = (
        "People in jail due to a pre-adjudication federal hold for U.S. Marshals Service, Federal Bureau of Prisons, "
        "or U.S. Immigration and Customs Enforcement"
    )
    TRIBAL_HOLD = "People in jail due to a pre-adjudication federal hold for a Tribal Nation or the Bureau of Indian Affairs"
    AWAITING_FAILURE_TO_APPEAR = "People held awaiting hearings for failure to appear in court or court-ordered programs"
    FAILURE_TO_PAY_FINES = "People held due to failure to pay fines or fees ordered by civil or criminal courts"
    OTHER_STATE_OR_COUNTY_HOLDS = "People held for other state or county jurisdictions"
    SERVING_JAIL_SENTENCE = "People in jail to serve a sentence of jail incarceration"
    SPLIT_JAIL_SENTENCE = (
        "People in jail to serve a split sentence of jail incarceration"
    )
    SUSPENDED_JAIL_SENTENCE = (
        "People in jail to serve a suspended sentence of jail incarceration"
    )
    REVOCATION_OF_SUPERVISION_SENTENCE = (
        "People in jail due to a revocation of post-adjudication community supervision sentence "
        "(i.e., probation, parole, or other community supervision sentence type)"
    )
    POST_ADJUDICATION_SANCTION_COMMUNITY_SUPERVISION = (
        "People in jail due to a post-adjudication incarceration sanction imposed by a community supervision agency "
        "(e.g., a “dip,” “dunk,” or weekend sentence)"
    )
    POST_ADJUDICATION_SANCTION_SPECIALTY_AGENCY = (
        "People in jail due to a post-adjudication incarceration sanction imposed by a specialty, treatment, "
        "or problem-solving court (e.g., a “dip,” “dunk,” or weekend sentence)"
    )


# Pre-adjudication Admissions
class PreAdjudicationAdmissionsIncludesExcludes(Enum):
    PAPER_BOOKINGS = "“Paper bookings” who are never admitted to facilities"
    LESS_THAN_TWELVE_HOURS = (
        "Admission events that are booked and released in less than 12 hours"
    )
    BETWEEN_TWELVE_AND_TWENTY_FOUR_HOURS = (
        "Admission events that are booked and released between 12 and 24 hours"
    )
    ADMITTED = "Admission events that are booked and admitted to jail pre-adjudication"
    TEMPORARY_ABSENCE = "Admission events returning from a temporary absence (e.g., hospital visit, court hearing, etc.)"
    MOVING = "Admission events that are moving between facilities under the same agency’s jurisdiction"


# Total Releases


class TotalReleasesIncludesExcludes(Enum):
    RELEASE_TO_OWN_RECOGNIZANCE = "Releases to own recognizance awaiting trial"
    MONETARY_BAIL = "Releases on monetary bail"
    NONMONETARY_BAIL = "Release events on nonmonetary bail (i.e., travel restrictions, no contact orders)"
    SUPERVISION_RELEASE = "Releases to supervision (including electronic monitoring, home confinement, traditional supervision, etc.)"
    BAIL_MODIFICATIONS = "Releases with subsequent bail modifications"
    DEATH_IN_CUSTODY = "Releases due to death in custody"
    ESCAPE_OR_AWOL = "Releases due to escape or Absent Without Leave (AWOL) status for more than 30 days"
    EMERGENCY_RELEASES = "Emergency releases (such as due to COVID-19, etc.)"
    PROBATION_SUPERVISION = "Releases to probation supervision following a period of jail incarceration (including electronic monitoring)"
    PAROLE_SUPERVISION = (
        "Releases to parole supervision following a period of jail incarceration"
    )
    OTHER_COMMUNITY_SUPERVISION = (
        "Releases to other community supervision that is not probation or parole"
    )
    INPATIENT_TREATMENT = "Releases to inpatient treatment in the community"
    NO_CORRECTIONAL_CONTROL = "Releases with no additional correctional control"
    TEMPORARY_RELEASES_EXCLUDE = "Temporary releases for medical or court appearances"


# Pre-adjudication Releases
class PreAdjudicationReleasesIncludesExcludes(Enum):
    AWAITING_TRIAL = "Releases to own recognizance awaiting trial"
    MONETARY_BAIL = "Releases on monetary bail"
    NON_MONETARY_BAIL = "Release events on nonmonetary bail (i.e., travel restrictions, no contact orders)"
    SUPERVISION = "Releases to supervision (including electronic monitoring, home confinement, traditional supervision, etc.)"
    BAIL_MODIFICATIONS = "Releases with subsequent bail modifications"
    DEATH = "Releases due to death in custody"
    AWOL = "Releases due to escape or Absent Without Leave (AWOL) status for more than 30 days"
    EMERGENCY = "Emergency releases (such as due to COVID-19, etc.)"


class PreAdjudicationReleasesOwnRecognizanceAwaitingTrialIncludesExcludes(Enum):
    SIGNATURE_BOND = (
        "Releases on signature bond (a.k.a. recognizance bond or oath bond)"
    )
    OWN_RECOGNIZANCE = "Releases on own recognizance"
    NON_MONETARY = "Releases on non-monetary conditions (e.g., surrender passport, no contact order, etc.)"


class PreAdjudicationReleasesMonetaryBailIncludesExcludes(Enum):
    UNSECURED_BOND = "Releases on unsecured bond"
    DEPOSIT_BOND = "Releases on deposit bond"
    MONETARY_BAIL = "Releases on monetary bail"
    BEFORE_HEARING = "Releases before initial bail hearing"


class PreAdjudicationReleasesDeathIncludesExcludes(Enum):
    UNDER_AGENCY_JURISDICTION = "Death while under the agency’s jurisdiction"
    TEMPORARILY_ABSENT = (
        "Death while temporarily absent (e.g., hospital, court, work release)"
    )


class PreAdjudicationReleasesEscapeOrAWOLIncludesExcludes(Enum):
    ESCAPE_PRETRIAL_HOLD = "Escape from pretrial hold"
    AWOL_PRETRIAL_HOLD = "AWOL from pretrial hold"
    AWOL_PRETRIAL_SUPERVISION = "AWOL from pretrial supervision"


# Post-adjudication Releases
class PostAdjudicationReleasesIncludesExcludes(Enum):
    PROBATION_SUPERVISION = "Releases to probation supervision following a period of jail incarceration (including electronic monitoring)"
    PAROLE_SUPERVISION = (
        "Releases to parole supervision following a period of jail incarceration"
    )
    COMMUNITY_SUPERVISION = (
        "Releases to other community supervision that is not probation or parole"
    )
    INPATIENT_TRATMENT = "Releases to inpatient treatment in the community"
    NO_ADDITIONAL_CONTROL = "Releases with no additional correctional control"


class PostAdjudicationReleasesProbationSupervisionIncludesExcludes(Enum):
    ADDITIONAL_PROBATION = (
        "Releases to an additional probation sentence after completing a jail sentence"
    )
    POST_JAIL_INCARCERATION = (
        "Releases back to probation after a jail incarceration probation sanction"
    )
    SPLIT_SENTENCE = "Releases to probation to serve a split- or on-and-after sentence"
    SHOCK_SENTENCE = "Releases to probation after a shock probation sentence"
    OTHER_COUNTY_STATE = (
        "Releases to probation in the jurisdiction of another county or state"
    )


class PostAdjudicationReleasesParoleSupervisionIncludesExcludes(Enum):
    PAROLE_BOARD = "Releases to parole at the authority of a parole board or similar decision-making entity"
    STATUTORY_REQUIREMENT = "Releases to parole by statutory requirement or other automatic release mechanism"
    POST_JAIL_SANCTION = (
        "Releases back to parole after a jail sanction for a parole violation"
    )
    COMMUTED_SENTENCE = "Releases to parole due to commuted or lowered sentence"
    OTHER_STATE = "Releases to parole or re-parole in the jurisdiction of another state"


class PostAdjudicationReleasesOtherCommunitySupervisionIncludesExcludes(Enum):
    POST_JAIL_SENTENCE = "Releases to an additional other community supervision sentence after completing a jail sentence"
    POST_JAIL_INCARCERATION = "Releases back to other community supervision after a jail incarceration probation sanction"
    SPLIT_SENTENCE = "Releases to other community supervision to serve a split- or on-and-after sentence"
    PAROLE_BOARD = "Releases to other community supervision at the authority of a parole board or similar decision-making entity"
    STATUTORY_REQUIREMENT = "Releases to other community supervision by statutory requirement or other automatic release mechanism"
    POST_JAIL_SANCTION = "Releases back to other community supervision after a jail sanction for a supervision violation"
    COMMUTED_SENTENCE = (
        "Releases to other community supervision due to commuted or lowered sentence"
    )
    OTHER_STATE = "Releases to other community supervision or re-released to other community supervision in the jurisdiction of another state"


class PostAdjudicationReleasesNoAdditionalCorrectionalControlIncludesExcludes(Enum):
    SENTENCE_COMPLETION = (
        "Releases due to sentence completion, no post-release supervision"
    )
    EXONERATION = "Releases due to exoneration"


class PostAdjudicationReleasesDueToDeathIncludesExcludes(Enum):
    IN_CUSTODY = "Releases due to death of people in custody"
    TEMPORARILY_ABSENT = "Releases due to death of people in custody who were temporarily absent (e.g., hospital, court, work release)"


class PostAdjudicationReleasesDueToEscapeOrAWOLIncludesExcludes(Enum):
    ESCAPE = "Escape from custody"
    AWOL = "AWOL from custody"


# Total Daily Population


class TotalDailyPopulationIncludesExcludes(Enum):
    """
    Enum representing various categories of individuals included or excluded in
    calculations of total jail admissions.
    """

    AWAITING_ARRAIGNMENT = "People in jail awaiting arraignment"
    UNPAID_BAIL = "People in jail due to unpaid bail"
    DENIAL_OF_BAIL = "People in jail due to denial of bail"
    REVOCATION_OF_BAIL = "People in jail due to revocation of bail"
    PENDING_CAPACITY_ASSESSMENT = (
        "People in jail pending assessment of capacity to stand trial"
    )
    TRANSFERRED_TO_HOSPITAL = "People who have been transferred to a hospital for a capacity assessment but are still counted on jail rolls"
    PENDING_PRETRIAL_REVOCATION = (
        "People in jail to be held pending outcome of pretrial revocation decision"
    )
    PRETRIAL_RELEASE_REVOCATION = "People in jail due to revocation of pretrial release"
    PRETRIAL_SUPERVISION_SANCTION = (
        "People in jail due to a pretrial supervision incarceration sanction"
    )
    FEDERAL_HOLD = (
        "People in jail due to a pre-adjudication federal hold for U.S. Marshals Service, Federal Bureau of Prisons, "
        "or U.S. Immigration and Customs Enforcement"
    )
    TRIBAL_NATION_HOLD = "People in jail due to a pre-adjudication federal hold for a Tribal Nation or the Bureau of Indian Affairs"
    FAILURE_TO_APPEAR = "People held awaiting hearings for failure to appear in court or court-ordered programs"
    FAILURE_TO_PAY_FEES = "People held due to failure to pay fines or fees ordered by civil or criminal courts"
    OTHER_JURISDICTION_HOLDS = "People held for other state or county jurisdictions"
    SERVING_SENTENCE = "People in jail to serve a sentence of jail incarceration"
    SPLIT_SENTENCE = "People in jail to serve a split sentence of jail incarceration"
    SUSPENDED_SENTENCE = (
        "People in jail to serve a suspended sentence of jail incarceration"
    )
    REVOCATION_OF_SUPERVISION = (
        "People in jail due to a revocation of post-adjudication community supervision sentence "
        "(i.e., probation, parole, or other community supervision sentence type)"
    )
    POST_ADJUDICATION_SUPERVISION_SANCTION = (
        "People in jail due to a post-adjudication incarceration sanction imposed by a community supervision agency "
        "(e.g., a “dip,” “dunk,” or weekend sentence)"
    )
    POST_ADJUDICATION_SPECIALTY_SANCTION = (
        "People in jail due to a post-adjudication incarceration sanction imposed by a specialty, treatment, "
        "or problem-solving court (e.g., a “dip,” “dunk,” or weekend sentence)"
    )


class MentalHealthNeedIncludesExcludes(Enum):
    SCREENING_TOOL = "Screening tool administered during their jail stay"
    CLINICAL_ASSESSMENT_JAIL = (
        "Clinical assessment or evaluation conducted during their jail stay"
    )
    SELF_REPORT = "Self-report"
    CLINICAL_ASSESSMENT_PRIOR = (
        "Clinical assessment or evaluation conducted prior to admission"
    )
    PRIOR_TREATMENT_HISTORY = "Prior history of receiving treatment"
    PSYCHOTROPIC_MEDICATION = "Psychotropic/psychiatric medication use"
    JAIL_CLASSIFICATION = (
        "Jail classification/housing assignment or programming received within the jail"
    )
    SUICIDE_RISK_SCREENING = "Positive risk screening for suicide ideation"
    OTHER_METHOD = "Other methodology"


class SubstanceUseNeedIncludesExcludes(Enum):
    SCREENING_TOOL = "Screening tool administered during their jail stay"
    CLINICAL_ASSESSMENT_JAIL = (
        "Clinical assessment or evaluation in jail conducted during their jail stay"
    )
    SELF_REPORT = "Self-report"
    POSITIVE_DRUG_TEST = "Positive drug test at admissions"
    CLINICAL_ASSESSMENT_PRIOR = (
        "Clinical assessment or evaluation conducted prior to admission"
    )
    PRIOR_TREATMENT_HISTORY = "Prior history of receiving treatment"
    MAT_RECEIVING = "Receiving Medication Assisted Treatment (MAT)"
    JAIL_CLASSIFICATION = (
        "Jail classification/housing assignment or programming received within the jail"
    )
    OTHER_METHOD = "Other methodology"


class CoOccurringSubstanceUseMentalHealthNeedIncludesExcludes(Enum):
    MENTAL_HEALTH_AND_SUBSTANCE_USE = (
        "People with identified mental health need AND substance use need"
    )
    ONLY_MENTAL_HEALTH = "People with identified mental health need only"
    ONLY_SUBSTANCE_USE = "People with identified substance use need only"
    SUBSTANCE_USE_AND_OTHER_NEED = "People with a substance use need and another need that is not mental health related"
    MENTAL_HEALTH_AND_OTHER_NEED = "People with an identified mental health need and another need that is not substance use related"


class OtherBehavioralHealthNeedIncludesExcludes(Enum):
    BEHAVIORAL_HEALTH_NEEDS = "People with identified behavioral health needs that does not meet the criteria for mental health or substance use need"
    COGNITIVE_DISORDERS = (
        "People with cognitive, neurocognitive/neurodevelopmental disorders"
    )
    CHRONIC_DISEASES = "People with chronic diseases"
    MENTAL_HEALTH_NEED = "People with identified mental health need"
    SUBSTANCE_USE_NEED = "People with identified substance use need"
    PSYCHOSOCIAL_PROBLEMS = "People with psychosocial and environmental problems"
    ECONOMIC_HOUSING_EDUCATION = (
        "People with economic, housing, or educational problems"
    )
    SOCIAL_SUPPORT_PROBLEMS = "People with problems with close relationships or with relating to their social environment"


class UnknownBehavioralHealthNeedIncludesExcludes(Enum):
    NOT_SCREENED_ASSESSED = (
        "People who have not been screened or assessed for behavioral health needs"
    )
    RELEASED_BEFORE_IDENTIFICATION = (
        "People who were released before a behavioral health need could be identified"
    )


class NoBehavioralHealthNeedIncludesExcludes(Enum):
    SCREENED_NO_NEED = "People who have been screened or assessed with no identified behavioral health needs"
    IDENTIFIED_MENTAL_HEALTH_NEED = "People with identified mental health needs"
    IDENTIFIED_SUBSTANCE_USE_NEED = "People with identified substance use needs"
    CO_OCCURRING_NEEDS = (
        "People with co-occurring substance use and mental health needs"
    )
