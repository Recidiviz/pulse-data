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
# TODO(#17577) implement multiple includes/excludes tables
class FundingIncludesExcludes(Enum):
    SINGLE_YEAR = "Funding for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )
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


class StateAppropriationIncludesExcludes(Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"
    GRANTS = "Grants from state sources that are not budget appropriations approved by the legislature/governor"


class CountyOrMunicipalAppropriationIncludesExcludes(Enum):
    FINALIZED = "Finalized county or municipal appropriations"
    PROPOSED = "Proposed county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class GrantsIncludesExcludes(Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE = "Private or foundation grants"


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
# TODO(#17577) Implement multiple includes/excludes tables
class ExpensesIncludesExcludes(Enum):
    SINGLE_YEAR = "Expenses for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )
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
