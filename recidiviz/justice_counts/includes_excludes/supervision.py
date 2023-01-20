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
"""Includes/Excludes definition for supervision agencies """

from enum import Enum

# Supervision Types
SUPERVISION_PROBATION_BREAKDOWN_DESCRIPTION = "People who are supervised in the community by a public or private probation agency. Probation is generally a sentence from a court that serves in lieu of incarceration. When probation follows incarceration, it differs from parole in that it does not provide early release from incarceration. Rather, it is a determinate sentence that follows a period of incarceration. Decisions to revoke probation are generally the responsibility of a court."
SUPERVISION_PAROLE_BREAKDOWN_DESCRIPTION = "People who are conditionally released from prison to serve the remainder of their sentence in the community. Parole releases may be determined by a parole board or by mandatory release according to statute. Decisions to revoke parole are generally the responsibility of a parole board."
SUPERVISION_PRETRIAL_BREAKDOWN_DESCRIPTION = "People who are supervised while awaiting trial as a condition of staying in the community until the disposition of their case. Decisions to revoke pretrial supervision are generally the responsibility of a court."
SUPERVISION_OTHER_COMMUNITY_BREAKDOWN_DESCRIPTION = "People who are under a type of community supervision, by a public or private agency, that is not probation, parole, or pretrial."


class SupervisionProbationDefinitionIncludesExcludes(Enum):
    IN_LIEU_INCARCERATION = "People sentenced to a period of probation in lieu of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    AFTER_INCARCERATION = "People sentenced to a period of probation after a period of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    POST_ADJUCATION_PROGRAM = "People on probation as part of a post-adjudication specialty or problem-solving court program (e.g., drug court)"
    TEMPORARILY_CONFINED = "People sentenced to probation who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    CONFINED_ANY_LENGTH = "People sentenced to probation confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    HOLD_PENDING = "People sentenced to probation who are in jail or prison on a hold pending resolution of a violation or revocation"
    LONGER_SANCTION = "People sentenced to probation who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    COMPACT_AGREEMENT = "People sentenced to probation in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    ANOTHER_JURISTICTION = (
        "People sentenced to probation who are being supervised by another jurisdiction"
    )
    IN_COMMUNITY = "People who have not been sentenced but are supervised on probation in the community prior to the resolution of their case"
    ANOTHER_FORM_SUPERVISION = (
        "People sentenced to probation who are also on another form of supervision"
    )
    PRE_ADJUCTATION_PROGRAM = "People on probation as part of a pre-adjudication specialty or problem-solving court program (e.g., drug court)"


class SupervisionParoleDefinitionIncludesExcludes(Enum):
    EARLY_RELEASE = "People approved by a parole board or similar entity for early conditional release from incarceration to parole supervision (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    STATUTORY_REQUIREMENT = "People conditionally released from incarceration to parole supervision by statutory requirement (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    TEMPORARILY_CONFINED = "People on parole who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    CONFINED_ANY_LENGTH = "People on parole confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    HOLD_PENDING = "People on parole who are in jail or prison on a hold pending resolution of a violation or revocation"
    LONGER_SANCTION = "People on parole who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    COMPACT_AGREEMENT = "People released to parole in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    ANOTHER_FORM_SUPERVISION = (
        "People on parole who are also on another form of supervision"
    )
    ANOTHER_JURISTICTION = (
        "People on parole who are being supervised by another jurisdiction"
    )


class SupervisionPretrialDefinitionIncludesExcludes(Enum):
    CITATION_RELEASE = "People on citation release (i.e., were never booked)"
    CONDITION_SUPERVISION = "People released from jail or otherwise not held pretrial on the condition of supervision (including electronic monitoring, home confinement, traditional supervision, etc.)"
    STATUTORY_REQUIREMENT = "People released from jail or otherwise not held pretrial due to statutory requirement"
    COURT_PROGRAM = "People supervised as part of a pre-adjudication specialty or problem-solving court program (e.g., drug court)"
    HOLD_PENDING = "People on pretrial supervision who are incarcerated on a hold pending resolution of a violation or revocation"
    ANOTHER_FORM_SUPERVISION = (
        "People on pretrial supervision who are also on another form of supervision"
    )


class SupervisionOtherCommunityDefinitionIncludesExcludes(Enum):
    IN_LIEU_INCARCERATION = "People sentenced to a period of other community supervision in lieu of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    DETERMINATE_PERIOD = "People sentenced to a determinate period of other community supervision after a period of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    POST_ADJUCATION_PROGRAM = "People on other community supervision as part of a post-adjudication specialty or problem-solving court program (e.g., drug court)"
    EARLY_RELEASE = "People approved by a parole board or similar entity for early conditional release from incarceration to other community supervision (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    STATUTORY_REQUIREMENT = "People conditionally released from incarceration to other community supervision by statutory requirement (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    TEMPORARILY_CONFINED = "People on other community supervision who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    CONFINED_ANY_LENGTH = "People on other community supervision confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    JAIL_OR_PRISON_HOLD_PENDING = "People on other community supervision who are in jail or prison on a hold pending resolution of a violation or revocation"
    LONGER_SANTION = "People on other community supervision who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    INCARCERATED_HOLD_PENDING = "People on other community supervision who are incarcerated on a hold pending resolution of a violation or revocation"
    COMPACT_AGREEMENT = "People on supervision in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    ANOTHER_FORM_SUPERVISION = "People on other community supervision who are also on another form of supervision"
    PRIOR_TO_RESOLUTION = "People on other community supervision who have not been sentenced but are supervised in the community prior to the resolution of their case"
    COURT_PROGRAM = "People on other community supervision in a pre-adjudication specialty or problem-solving court program (e.g., drug court, etc.)"


# Global Definitions
class SupervisionPersonChargesOffensesIncludesExcludes(Enum):
    """Includes/Excludes class for Person Charges/Offenses"""

    AGGRAVATED_ASSAULT = "Aggravated assault"
    SIMPLE_ASSAULT = "Simple assault"
    INTIMIDATION = "Intimidation"
    MURDER = "Murder and nonnegligent manslaughter"
    NEGLIGENT_MANSLAUGHTER = "Negligent manslaughter"
    COMMERCIAL_SEX_ACTS = "Human trafficking, commercial sex acts"
    INVOLUNTARY_SERVITUDE = "Human trafficking, involuntary servitude"
    KIDNAPPING = "Kidnapping/abduction"
    RAPE = "Rape"
    SODOMY = "Sodomy"
    SEXUAL_ASSAULT_OBJECT = "Sexual assault with an object"
    FONDLING = "Fondling"
    INCEST = "Incest"
    STATUTORY_RAPE = "Statutory rape"
    ROBBERY = "Robbery"
    JUSTIFIABLE_HOMICIDE = "Justifiable homicide"


class SupervisionPropertyChargesOffensesIncludesExcludes(Enum):
    """Includes/Excludes class for Property Charges/Offenses"""

    ARSON = "Arson"
    BRIBERY = "Bribery"
    BURGLARY = "Burglary/breaking and entering"
    COUNTERFEITING = "Counterfeiting/forgery"
    VANDALISM = "Destruction/damage/vandalism of property"
    EMBEZZLEMENT = "Embezzlement"
    EXTORTION = "Extortion/blackmail"
    FALSE_PRETENSES = "False pretenses/swindle/confidence game"
    CREDIT_CARD_FRAUD = "Credit card/automated teller machine fraud"
    IMPERSONATION = "Impersonation"
    WELFARE_FRAUD = "Welfare fraud"
    WIRE_FRAUD = "Wire fraud"
    IDENTITY_THEFT = "Identify theft"
    HACKING_INVASION = "Hacking/computer invasion"
    POCKET_PICKING = "Pocket-picking"
    PURSE_SNATCHING = "Purse-snatching"
    SHOPLIFTING = "Shoplifting"
    THEFT_FROM_BUILDING = "Theft from building"
    THEFT_FROM_MACHINE = "Theft from coin-operated machine or device"
    THEFT_FROM_MOTOR_VEHICLE = "Theft from motor vehicle"
    THEFT_MOTOR_VEHICLE_PARTS = "Theft of motor vehicle parts or accessories"
    LARCENY = "All other larceny"
    MOTOR_VEHICLE_THEFT = "Motor vehicle theft"
    STOLEN_PROPERTY = "Stolen property offenses"
    ROBBERY = "Robbery"


class SupervisionPublicOrderChargesOffensesIncludesExcludes(Enum):
    """Includes/Excludes class for Public Order Charges/Offenses"""

    ANIMAL_CRUELTY = "Animal cruelty"
    IMPORT_VIOLATIONS = "Import violations"
    EXPORT_VIOLATIONS = "Export violations"
    FEDERAL_LIQUOR_OFFENSES = "Federal liquor offenses"
    FEDERAL_TOBACCO_OFFENSES = "Federal tobacco offenses"
    WILDLIFE_TRAFFICKING = "Wildlife trafficking"
    ESPIONAGE = "Espionage"
    MONEY_LAUNDERING = "Money laundering"
    HARBORING_ESCAPEE = "Harboring escapee/concealing from arrest"
    FLIGHT_TO_AVOID_PROSECUTION = "Flight to avoid prosecution"
    FLIGHT_TO_AVOID_DEPORTATION = "Flight to avoid deportation"
    BETTING = "Betting/wagering"
    ASSISTING_GAMBLING = "Operating/promoting/assisting gambling"
    GAMBLING_EQUIPMENT_VIOLATIONS = "Gambling equipment violations"
    SPORTS_TAMPERING = "Sports tampering"
    ILLEGAL_ENTRY = "Illegal entry into the United States"
    FALSE_CITIZENSHIP = "False citizenship"
    SMUGGLING = "Smuggling aliens"
    RE_ENTRY_AFTER_DEPORTATION = "Re-entry after deportation"
    PORNOGRAPHY = "Pornography/obscene material"
    PROSTITUTION = "Prostitution"
    ASSISTING_PROSTITUTION = "Assisting or promoting prostitution"
    PURCHASING_PROSTITUTION = "Purchasing prostitution"
    TREASON = "Treason"
    WEAPON_LAW_VIOLATIONS = "Weapon law violations"
    VIOLATION_NATIONAL_FIREARM_ACT = "Violation of National Firearm Act of 1934"
    WEAPONS_OF_MASS_DESTRUCTION = "Weapons of mass destruction"
    EXPLOSIVES = "Explosives"
    FAILURE_TO_APPEAR = "Failure to appear"
    CURFEW_VIOLATIONS = "Curfew/loitering/vagrancy violations"
    DISORDERLY_CONDUCT = "Disorderly conduct"
    DRIVING_UNDER_INFLUENCE = "Driving under the influence"
    FAMILY_OFFENSES_NONVIOLENT = "Family offenses, nonviolent"
    FEDERAL_RESOURCE_VIOLATIONS = "Federal resource violations"
    LIQUOR_LAW_VIOLATIONS = "Liquor law violations"
    PERJURY = "Perjury"
    TRESPASS_OF_REAL_PROPERTY = "Trespass of real property"
    DRUG_VIOLATIONS = "Drug/narcotic violations"
    DRUG_EQUIPMENT_VIOLATIONS = "Drug equipment violations"
    DRUG_SALES = "Drug sales"
    DRUG_DISTRIBUTION = "Drug distribution"
    DRUG_MANUFACTURING = "Drug manufacturing"
    DRUG_SMUGGLING = "Drug smuggling"
    DRUG_PRODUCTION = "Drug production"
    DRUG_POSSESSION = "Drug possession"


class SupervisionDrugChargesOffensesIncludesExcludes(Enum):
    """Includes/Excludes class for Drug Charges/Offenses"""

    DRUG_NARCOTIC_VIOLATIONS = "Drug/narcotic violations"
    DRUG_EQUIPMENT_VIOLATIONS = "Drug equipment violations"
    DRUG_SALES = "Drug sales"
    DRUG_DISTRIBUTION = "Drug distribution"
    DRUG_MANUFACTURING = "Drug manufacturing"
    DRUG_SMUGGLING = "Drug smuggling"
    DRUG_PRODUCTION = "Drug production"
    DRUG_POSSESSION = "Drug possession"


# Funding
# TODO(#17577)
class SupervisionFundingIncludesExcludes(Enum):
    SINGLE_FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR_APPROPRIATIONS = (
        "Multi-year appropriations that are appropriated in during the time period"
    )
    COMMUNITY_SUPERVISION_OFFICE = (
        "Funding for community supervision office facility operations and maintenance"
    )
    COMMUNITY_SUPERVISION_CONFINEMENT = "Funding for community supervision confinement facility operations and maintenance (e.g., violation centers, halfway back facilities, etc.)"
    CONSTRUCTION_RENTAL = (
        "Funding for construction or rental of new community supervision facilities"
    )
    TREATMENT_PROGRAMMING = (
        "Funding for agency-run or contracted treatment and programming"
    )
    SUPERVISION_STAFF = "Funding for community supervision staff"
    SUPPORT_STAFF = "Funding for central administrative and support staff"
    PRIVATE_SERVICES = "Funding for the operation of private community supervision services contracted by the agency"
    INTERSTATE_COMPACT = "Funding for supervision services contracted to other jurisdictions through interstate compact"
    STIPENDS_JAIL = "Funding for stipends or reimbursements for people on supervision detained in jail facilities (locally or out of state)"
    STIPENDS_PRISON = "Funding for stipends or reimbursements for people on supervision detained in prison facilities (locally or out of state)"
    JAIL_MAINTENANCE = "Funding for jail facility operations and maintenance"
    PRISON_MAINTENANCE = "Funding for prison facility operations and maintenance"
    JUVENILE_SUPERVISION = "Funding for juvenile supervision"


class SupervisionStateAppropriationIncludesExcludes(Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"
    GRANTS_NOT_BUDGET = "Grants from state sources that are not budget appropriations approved by the legislature/governor"


class SupervisionCountyMunicipalAppropriationIncludesExcludes(Enum):
    FINALIZED = "Finalized county or municipal appropriations"
    PROPOSED = "Proposed county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class SupervisionGrantsIncludesExcludes(Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE = "Private or foundation grants"


class SupervisionFinesFeesIncludesExcludes(Enum):
    SUPERVISION_FEED = "Supervision fees"
    ELECTRONIC_MONITORING = (
        "Fees charged to people on supervision for electronic monitoring"
    )
    PROGRAMMING = "Fees charged to people on supervision for programming"
    TREATMENT = "Fees charged to people on supervision for treatment"
    TESTING = "Fees charged to people on supervision for drug or alcohol testing"
    RESTITUTION = "Restitution"
    LEGAL_OBLIGATIONS = "Legal financial obligations"


# Expenses
# TODO(#17577)
class SupervisionExpensesIncludesExcludes(Enum):
    """Includes/Excludes class for Supervision Expenses"""

    SINGLE_YEAR = ("Expenses for single fiscal year",)
    BIENNIUM = ("Biennium funding allocated during the time period",)
    MULTI_YEAR = ("Multi-year appropriations allocated in during the time period",)
    FACILITY_MAINTENANCE = (
        "Expenses for community supervision office facility operations and maintenance",
    )
    CONFINEMENT_FACILITY = (
        "Expenses for community supervision confinement facility operations and maintenance (e.g., violation centers, halfway back facilities, etc.)",
    )
    RENTAL_FACILITIES = (
        "Expenses for construction or rental of new community supervision facilities",
    )
    TREATMENT_PROGRAMMING = (
        "Expenses for agency-run or contracted treatment and programming",
    )
    SUPERVISION_STAFF = ("Expenses for community supervision staff",)
    SUPPORT_STAFF = ("Expenses for central administrative and support staff",)
    PRIVATE_SERVICES = (
        "Expenses for the operation of private community supervision services contracted by the agency",
    )
    INTERSTATE_COMPACT = (
        "Expenses for supervision services contracted to other jurisdictions through interstate compact",
    )
    STIPENDS_JAILS = (
        "Expenses for stipends or reimbursements for people on supervision detained in jail facilities (locally or out of state)",
    )
    STIPENDS_PRISONS = (
        "Expenses for stipends or reimbursements for people on supervision detained in prison facilities (locally or out of state)",
    )
    JAILS = ("Expenses for jail facility operations and maintenance",)
    PRISONS = ("Expenses for prison facility operations and maintenance",)
    JUVENILE_SUPERVISION = ("Expenses for juvenile supervision",)


class SupervisionPersonnelExpensesIncludesExcludes(Enum):
    SALARIES = "Salaries"
    BENEFITS = "Benefits"
    RETIREMENT = "Retirement contributions"
    INDIVIDUALS_CONTRACTED = (
        "Costs for individuals contracted to work for the supervision agency"
    )
    COMPANIES_CONTRACTED = "Costs for companies or service providers contracted to support work with supervision agencies"


class SupervisionTrainingExpensesIncludesExcludes(Enum):
    ANNUAL = "Annual training"
    CONTINUING = "Continuing education"
    ACADEMY = "Training academy"
    SPECIALIZED = "Specialized training"
    EXTERNAL = "External training or professional development opportunities (conferences, classes, etc.)"


class SupervisionFacilitiesEquipmentExpensesIncludesExcludes(Enum):
    OPERATIONS = "Supervision facility operations"
    MAINTENANCE = "Supervision facility maintenance"
    RENOVATION = "Supervision facility renovation"
    CONSTRUCTION = "Supervision facility construction"
    EQUIPMENT = "Equipment (e.g., computers, communication, and information technology infrastructure)"


# Staff
class SupervisionStaffIncludesExcludes(Enum):
    FILLED = "Filled positions"
    VACANT = "Staff positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions"
    PART_TIME = "Part-time positions"
    CONTRACTED = "Contracted positions"
    TEMPORARY = "Temporary positions"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"


class SupervisionStaffDimIncludesExcludes(Enum):
    OFFICERS = "Supervision officers (with caseloads)"
    SUPERVISORS = "Supervision supervisors (with caseloads)"
    VACANT = "Any supervision staff positions budgeted but currently vacant"


class SupervisionManagementOperationsStaffIncludesExcludes(Enum):
    MANAGEMENT = "Supervision agency management (i.e., district managers, regional managers who do not carry caseloads as a primary job function)"
    CLERICAL_OR_ADMIN = "Clerical or administrative staff"
    MAINTENANCE = "Maintenance staff"
    RESEARCH = "Research staff"
    VACANT = "Management and operations staff positions budgeted but currently vacant"


class SupervisionClinicalMedicalStaffIncludesExcludes(Enum):
    MEDICAL_DOCTORS = "Medical doctors"
    NURSES = "Nurses"
    DENTISTS = "Dentists"
    CLINICIANS = "Clinicians (e.g., substance use treatment specialists)"
    THERAPISTS = "Therapists (e.g., mental health counselors)"
    PSYCHIATRISTS = "Psychiatrists"
    VACANT = "Clinical and medical staff positions budgeted but currently vacant"


class SupervisionProgrammaticStaffIncludesExcludes(Enum):
    VOCATIONAL = "Vocational staff"
    EDUCATIONAL = "Educational staff"
    THERAPUTIC_AND_SUPPORT = "Therapeutic and support program staff"
    RELIGIOUS = "Religious or cultural program staff"
    VOLUNTEER = "Programmatic staff volunteer positions"
    VACANT = "Programmatic staff positions budgeted but currently vacant"


class SupervisionVacantStaffIncludesExcludes(Enum):
    VACANT_SUPERVISION = "Vacant supervision staff positions"
    VACANT_MANAGEMENT_AND_OPS = "Vacant management and operations"
    VACANT_CLINICAL_AND_MEDICAL = "Vacant clinical and medical staff positions"
    VACANT_PROGRAMMATIC = "Vacant programmatic staff positions"
    VACANT_UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"


# Daily Population


class PeopleOnActiveSupervisionIncludesExcludes(Enum):
    OFFICE_VISITS = "People whose supervision includes office visits"
    HOME_WORK_VISITS = "People whose supervision includes home or work visits"
    DRUG_ALCOHOL_TESTING = "People whose supervision includes drug or alcohol testing"
    PROGRAMMING_OR_TREATMENT = (
        "People whose supervision includes participation in programming or treatment"
    )
    COMMUNITY_SERVICE = "People whose supervision includes community service"
    ELECTRONIC_MONITORING = "People whose supervision includes electronic monitoring in addition to regular contact with agency staff"
    TELEPHONE_MAIL_CONTACTS = (
        "People whose supervision includes only telephone or mail contacts"
    )


class PeopleOnAdministrativeSupervisionIncludesExcludes(Enum):
    TELEPHONE_MAIL_CONTACTS = (
        "People whose supervision includes only telephone or mail contacts"
    )
    LEGAL_FINANCIAL_OBLIGATIONS = "People whose supervision is limited to monitoring payments of legal financial obligations"
    RESTITUTION = (
        "People whose supervision is limited to monitoring payments of restitution"
    )
    ELECTRONIC_MONITORING = (
        "People whose supervision includes only electronic monitoring"
    )


class PeopleAbscondedSupervisionIncludesExcludes(Enum):
    FAILED_TO_REPORT = "People who failed to report upon release from incarceration"
    STOPPED_REPORTING = (
        "People who stopped reporting after a period of successful supervision"
    )
    MOVED = "People who have moved from their approved residence and whose whereabouts are unknown"
    UNSUCCESSFUL_LOCATE = "People for whom the agency’s efforts to locate via letters, home visits, or family or work calls were unsuccessful"


class PeopleIncarceratedOnHoldSanctionSupervisionIncludesExcludes(Enum):
    HELD_JAIL = "People held in jail"
    HELD_PRISON = "People held in prison"
    HELD_FACILITY = "People held in a residential treatment or programming facility"
    HELD_CONFINEMENT = "People held in a confinement facility under the jurisdiction of the supervision agency (e.g., violation center or halfway back facility, etc.)"
    REVOKED_TO_PRISON_JAIL = "People who are revoked to prison or jail who are no longer on supervision under the jurisdiction of the agency"


# Violations
class SupervisionViolationsIncludesExcludes(Enum):
    AGENCY_SANCTION = "Violation incidents resulting in the application of a supervision agency sanction (increased reporting, curfew, etc.)"
    COURT_SANCTION = "Violation incidents resulting in a court sanction (remand to custody, court-ordered treatment, etc.)"
    REVOCATION = "Violation incidents resulting in revocation of supervision"


class SupervisionTechnicalViolationsIncludesExcludes(Enum):
    CRIMINAL_ASSOCIATION = "Criminal association violation"
    EMPLOYMENT = "Employment violation"
    FINANCIAL_OBLIGATIONS = (
        "Financial obligations violation (e.g., legal, restitution, child support)"
    )
    FIREARM = "Firearm violation"
    REPORTING = "Reporting violation"
    FAILURE_TO_APPEAR = "Failure to appear violation"
    RESIDENCY_OR_HOUSING = "Residency or housing violation"
    RESTRAINING = "Restraining or protective order violation"
    SPECIAL_CONDITIONS = "Special conditions violation"
    SUBSTANCE_USE = "Substance use violation"
    DIRECTIVES = "Supervision directives violation"
    FEES = "Supervision fees violation"
    TRAVEL = "Travel violation"
    WEAPONS = "Weapons violation (non-firearm)"
    CRIMINAL_OFFENSE = "Admission of criminal offense (no arrest)"
    ARREST = "Arrest for new criminal charge"
    CONVICTION = "Conviction for new criminal charge"
    ABSCONDING = "Absconding violation"


class SupervisionAbscondingViolationsIncludesExcludes(Enum):
    ABSCONDING = "Absconding violation"


class SupervisionNewOffenseViolationsIncludesExcludes(Enum):
    DISCLOSURE = "Disclosure of criminal offense (no arrest)"
    ARREST = "Arrest for new criminal charge"
    CONVICTION = "Conviction for new criminal charge"


# New Cases
class SupervisionNewCasesIncludesExcludes(Enum):
    ACTIVE = "People with new active supervision cases"
    ADMINISTRATIVE = "People with new administrative supervision cases"
    NON_REPORTING = "People with new non-reporting cases (e.g., no fees, no reporting, no travel restrictions)"
    SPECIALIZED = "People with new specialized cases (e.g., for sex offenses, domestic violence, serious mental illness)"
    PRETRIAL_INVESTIGATION = "People with new cases in pretrial investigation"
    TRANSFERRED = "People with cases transferred between supervision districts or supervision officers in the same jurisdiction"


# Discharges
class SupervisionDischargesIncludesExcludes(Enum):
    SUCCESSFUL = "Successful completions of supervision"
    UNSUCCESSFUL = "Unsuccessful discharge from supervision"
    NEUTRAL = "Neutral discharge from supervision"
    TRANSFERRED = "People transferred between supervision districts or supervision officers in the same jurisdiction"


class SupervisionSuccessfulCompletionIncludesExcludes(Enum):
    COMPLETED_REQUIREMENTS = (
        "People who completed all requirements of supervision and were released"
    )
    EARLY_RELEASE = (
        "People who were granted early release from supervision for earned or good time"
    )
    END_OF_TERM = "People who arrive at the end of their supervision term in good standing and without revocation"
    OUTSTANDING_VIOLATIONS = "People who completed their full term of their supervision sentence, but have outstanding supervision violations pending resolution"
    ABSCONSCION = "People who were discharged from supervision due to a prolonged period of absconsion"
    DIED = "People who died during their term of supervision"
    INCARCERATION = "People whose supervision term was terminated due to failure to meet the requirements of supervision, resulting in incarceration (e.g., pretrial release revocation probation revocation, etc.)"
    REVOKED = "People who were revoked from one kind of supervision to another (e.g., people revoked to regular probation from a supervision term that, when completed, would result in no criminal record)"
    TERMINATED = "People who were determined to not be a match for supervision and terminated from the agency’s jurisdiction"


class SupervisionNeutralDischargeIncludesExcludes(Enum):
    OUTSTANDING_VIOLATIONS = "People who completed their full term of their supervision sentence, but have outstanding supervision violations pending resolution"
    ABSCONSCION = "People who were discharged from supervision due to a prolonged period of absconsion"
    DIED = "People who died during their term of supervision"
    INCARCERATION = "People whose supervision term was terminated due to failure to meet the requirements of supervision, resulting in incarceration (e.g., pretrial release revocation probation revocation, etc.)"
    REVOKED = "People who were revoked from one kind of supervision to another (e.g., people revoked to regular probation from a supervision term that, when completed, would result in no criminal record)"
    TERMINATED = "People who were determined to not be a match for supervision and terminated from the agency’s jurisdiction"
    COMPLETED_REQUIREMENTS = (
        "People who completed all requirements of supervision and were released"
    )
    EARLY_RELEASE = (
        "People who were granted early release from supervision for earned or good time"
    )
    END_OF_TERM = "People who arrive at the end of their supervision term in good standing and without revocation"


class SupervisionUnsuccessfulDischargeIncludesExcludes(Enum):
    INCARCERATION = "People whose supervision term was terminated due to failure to meet the requirements of supervision, resulting in incarceration (e.g., pretrial release revocation probation revocation, etc.)"
    REVOKED = "People who were revoked from one kind of supervision to another (e.g., people revoked to regular probation from a supervision term that, when completed, would result in no criminal record)"
    TERMINATED = "People who were determined to not be a match for supervision and terminated from the agency’s jurisdiction"
    OUTSTANDING_VIOLATIONS = "People who completed their full term of their supervision sentence, but have outstanding supervision violations pending resolution"
    ABSCONSCION = "People who were discharged from supervision due to a prolonged period of absconsion"
    DIED = "People who died during their term of supervision"
    COMPLETED_REQUIREMENTS = (
        "People who completed all requirements of supervision and were released"
    )
    EARLY_RELEASE = (
        "People who were granted early release from supervision for earned or good time"
    )
    END_OF_TERM = "People who arrive at the end of their supervision term in good standing and without revocation"


# Reconvictions
class SupervisionReconvictionsIncludesExcludes(Enum):
    NEW_FELONY = "People with a new felony conviction"
    NEW_MISDEMEANOR = "People with a new misdemeanor conviction"
    NEW_INFRACTION = "People with a new infraction conviction"


# Caseload
class SupervisionCaseloadNumeratorIncludesExcludes(Enum):
    PEOPLE_ACTIVE = "People with cases on active supervision status"
    PEOPLE_ADMINISTRATIVE = "People with cases on administrative status"
    PEOPLE_ABSCONDER = "People with cases on absconder status"
    PEOPLE_NON_REPORTING = "People with cases on non-reporting status (e.g., no fees, no reporting, no travel restrictions)"
    PEOPLE_SPECIALIZED = "People with cases on specialized caseloads (e.g., for sex offenses, domestic violence, serious mental illness)"
    PEOPLE_PRETRIAL = "People with cases in pretrial investigation"


class SupervisionCaseloadDenominatorIncludesExcludes(Enum):
    SUPERVISION_STAFF = "Supervision staff carrying a caseload"
    SUPERVISION_SUPERVISORS = "Supervision supervisors carrying a caseload"
    NON_SUPERVISION_STAFF = (
        "Non-supervision administrative staff temporarily carrying a caseload"
    )
    STAFF_ON_LEAVE = "Staff on leave whose caseload is being covered by a colleague"


# Revocations
class SupervisionRevocationsIncludesExcludes(Enum):
    PRISON = "Revocation to prison"
    JAIL = "Revocation to jail"
    INPATIENT_TREATMENT = "Revocation to inpatient treatment in the community"
    FACILITY = "Revocation to treatment in a prison or jail facility"
    NEW_SUPERVISION = "Revocation to a new supervision term"
    TERMINATION = "Revocation to supervision termination"
    SHORT_TERM_INCARCERATION = "Short-term incarceration with a return to the same term of supervision (e.g., “dips,” “dunks,” etc.)"
