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
"""Includes/Excludes definition for court agencies """

import enum

# Judges and Staff


class JudgesIncludesExcludes(enum.Enum):
    WITH_CRIMINAL_CASE = "Judges with a criminal case docket"
    WITHOUT_CRIMINAL_CASE = "Judges without a criminal case docket"


class LegalStaffIncludesExcludes(enum.Enum):
    CLERKS = "Law clerks"
    ATTORNEYS = "Staff attorneys"
    PARALEGALS = "Paralegals"
    JUDGES = "Judges"


class SecurityStaffIncludesExcludes(enum.Enum):
    BAILIFFS = "Bailiffs"
    POLICE_OFFICERS = "Court police officers"


class SupportOrAdministrativeStaffIncludesExcludes(enum.Enum):
    CLERKS = "Clerks"
    ANALYTICS = "Data, research, analytics staff"
    IT = "Information technology staff"
    BUDGETARY = "Budgetary staff"
    HR = "Human resources staff"
    RECORDERS = "Court reporters or recorders"


class VictimAdvocateStaffIncludesExcludes(enum.Enum):
    LEGAL = "Victim advocate staff in legal roles"
    NON_LEGAL = "Victim advocate staff in non-legal roles (e.g., witness support services, victim advocacy case managers, etc.)"
    NOT_FULL_TIME = "Staff trained in victim advocacy support who do not perform these roles full-time"


class VacantPositionsIncludesExcludes(enum.Enum):
    JUDGE = "Vacant judge positions"
    NON_JUDICIAL = "Vacant non-judicial legal staff positions"
    SECURITY = "Vacant security staff positions"
    ADMINISTRATIVE = "Vacant support or administrative staff positions"
    ADVOCATE = "Vacant victim advocate staff positions"
    UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"


# Sentences Imposed
class SentencesImposedIncludesExcludes(enum.Enum):
    NEW_SENTENCE = "Cases resulting in a new sentence imposed (e.g., new conviction, parole revocation, etc.)"
    SUSPENDED_SENTENCE = "Cases involving the imposition of some or all of the incarceration portion of a suspended sentence"
    SENTENCE_CHANGE = "Cases in which the sentence is changed by the court but the conviction remains in place"
    TIME_SERVED = "Cases in which the sentence imposed is for time served"
    TRANSFERRED = "Cases transferred to another jurisdiction"
    REINSTATED = "Cases involving a person reinstated to an existing community supervision sentence"
    CHANGING_PAROLE_STATUS = "Cases involving a person changing parole status (e.g., returning to prison from parole or re-paroled to the community)"


class PrisonSentencesIncludesExcludes(enum.Enum):
    INCARCERATION = "Cases resulting in a sentence of incarceration in state prison"
    RETURNS_TO_PRISON = "Court-ordered returns to state prison for violations of the conditions of community supervision or revocations"
    SPLIT_SENTENCE = "Convictions that result in a split sentence of incarceration in state prison followed by a period of community supervision that is not the result of early release to the community while still under correctional control for the duration of a prison term (e.g. parole supervision)"


class JailSentencesIncludesExcludes(enum.Enum):
    INCARCERATION = (
        "Convictions that result in a sentence of incarceration in county jail"
    )
    TIME_SERVED = "Convictions that result in a sentence of incarceration in county jail that concludes on imposition due to time served"
    RETURNS_TO_JAIL = "Court ordered returns to county jail for violations or revocations of the conditions of community supervision"
    SPLIT_SENTENCE = "Convictions that result in a split sentence of incarceration in county jail followed by a period of community supervision that is not the result of early release to the community while still under correctional control for the duration of a jail term (e.g. parole supervision)"


class SplitSentencesIncludesExcludes(enum.Enum):
    SPLIT_SENTENCE_PRISON = "Convictions that result in a split sentence of incarceration in state prison and a period of community supervision that is not the result of early release to the community while still under correctional control for the duration of a prison term (e.g. parole supervision)"
    SPLIT_SENTENCE_JAIL = "Convictions that result in a split sentence of incarceration in county jail and a period of community supervision that is not the result of early release to the community while still under correctional control for the duration of a prison term (e.g. parole supervision)"


class SuspendedSentencesIncludesExcludes(enum.Enum):
    PROBATION = "Convictions that result in a suspended sentence of incarceration where the community supervision term is served on probation"
    COMMUNITY_SUPERVISION = "Convictions that result in a suspended sentence of incarceration where the community supervision term is served on a type of community supervision that is not probation"
    VIOLATION = "Convictions that result in the incarceration portion of a suspended sentence being imposed due to violation of the conditions of community supervision"


class CommunitySupervisionOnlySentencesIncludesExcludes(enum.Enum):
    PROBATION = "Convictions that result in a sentence of probation only"
    COMMUNITY_CORRECTIONS = (
        "Convictions that result in a sentence of community corrections only"
    )
    COMMUNITY_SUPERVISION = "Convictions that result in a sentence to community supervision that is not probation or parole"
    SPLIT_SENTENCE = "Convictions that result in a split sentence"
    SUSPENDED_SENTENCE = "Convictions that result in a suspended sentence"


class FinesOrFeesOnlySentencesIncludesExcludes(enum.Enum):
    FINE_FEE = "Convictions that solely result in a fine or fee"
    PAYMENTS_VICTIM_RESTITUTION = (
        "Convictions that solely result in payments of victim restitution"
    )
    CASE_FEES = (
        "Fees only imposed during the course of a case (e.g., drug testing fees)"
    )
    MONETARY_SANCTIONS = (
        "Convictions that result in monetary sanctions in addition to other sanctions"
    )
    OTHER_FINANCIAL_OBLIGATIONS = "Convictions that result in other financial obligations not captured in the listed categories"


# New Offenses While on Pretrial Release
class NewOffensesWhileOnPretrialReleaseIncludesExcludes(enum.Enum):
    OWN_RECOGNIZANCE = "Cases involving people released on their own recognizance"
    MONETARY_BAIL = "Cases involving people released on monetary bail"
    NON_MONETARY_BAIL = "Cases involving people released on non-monetary bail"
    BAIL_MODIFICATION = (
        "Cases involving people released with subsequent bail modifications"
    )
    AWAITING_DISPOSITION = "Cases involving people initially ordered by the court to be held awaiting disposition"
    TRANSFERRED = "Cases involving people transferred to another jurisdiction"


# Criminal Case Filings
class CriminalCaseFilingsIncludesExcludes(enum.Enum):
    AUTHORIZED_AGENCY = "New cases filed by any authorized agency (e.g., prosecuting authority, law enforcement agency, etc.)"
    NEW_CHARGES = (
        "Cases filed for new criminal charges for people on community supervision"
    )
    TRANSFERRED_EXTERNAL = (
        "Cases transferred from another jurisdiction for new prosecution"
    )
    VIOLATIONS = "Violations of an existing supervision case"
    REVOCATIONS = "Revocations of an existing supervision case"
    REOPENED = "Inactive cases reopened"
    TRANSFERRED_INTERNAL = "Cases transferred internally"


class FelonyCriminalCaseFilingsIncludesExcludes(enum.Enum):
    FELONY_CHARGE = "Cases with a leading felony charge"


class MisdemeanorOrInfractionCriminalCaseFilingsIncludesExcludes(enum.Enum):
    MISDEMEANOR_CHARGE = "Cases with a leading misdemeanor charge"
    INFRACTION_CHARGE = "Cases with a leading infraction charge"


# Cases Overturned on Appeal
class CasesOverturnedOnAppealIncludesExcludes(enum.Enum):
    OVERTURNED = "Cases overturned on appeal"
    INTERLOCUTORY_APPEAL = "Cases involving interlocutory appeal"


# Pretrial Releases
class PretrialReleasesIncludesExcludes(enum.Enum):
    ON_OWN = "People released on their own recognizance"
    MONETARY_BAIL = "People released on monetary bail"
    NON_MONETARY_BAIL = "People released on non-monetary bail"
    BAIL_MODIFICATION = "People released with subsequent bail modifications"
    AWAITING_DISPOSITION = (
        "People initially ordered by the court to be held awaiting disposition"
    )
    TRANSFERRED = "People transferred to another jurisdiction"


class PretrialReleasesOnOwnRecognizanceIncludesExcludes(enum.Enum):
    OWN_RECOGNIZANCE = "People released on their own recognizance"
    SIGNATURE_BOND = (
        "People released on a signature bond (a.k.a. recognizance bond or oath bond)"
    )
    STATUTORY_REQUIREMENT = "People released due to a statutory requirement"
    BEFORE_BAIL_HEARING = "People released before initial bail hearing"
    AWAITING_DISPOSITION = "People held awaiting disposition"
    TRANSFERRED = "People transferred to another jurisdiction"


class PretrialReleasesMonetaryBailIncludesExcludes(enum.Enum):
    UNSECURED = "People released on unsecured bond"
    DEPOSIT = "People released on deposit bond"
    MONETARY_BAIL = "People released on monetary bail"
    BEFORE_BAIL_HEARING = "People released before initial bail hearing"


class PretrialReleasesNonMonetaryBailIncludesExcludes(enum.Enum):
    WITH_PRE_TRIAL_SUPERVISION = (
        "People released with any form of pre-trial supervision"
    )
    WITHOUT_PRE_TRIAL_SUPERVISION = (
        "People released without any form of pre-trial supervision"
    )


# Funding
# TODO(#17577) implement multiple includes/excludes tables
class FundingIncludesExcludes(enum.Enum):
    FISCAL_YEAR = "Funding for single fiscal year"
    BIENNIUM = "Biennium funding appropriated during the time period"
    MULTI_YEAR = (
        "Multi-year appropriations that are appropriated in during the time period"
    )
    FACILITY_OPERATIONS_MAINTENANCE = (
        "Funding for court system facility operations and maintenance"
    )
    CONSTRUCTION = "Funding for construction or rental of new court system facilities"
    PROGRAMMING = (
        "Funding for court system-run or -contracted treatment and programming"
    )
    PRETRIAL_SERVICES = "Funding for pretrial services managed by the court system"
    STAFF = "Funding for court system staff"
    CASE_PROCESSING = "Funding for criminal case processing"
    CASE_MANAGEMENT = "Funding for electronic case management systems"
    SUPERVISION_OPERATIONS = (
        "Funding for community supervision operations and facility maintenance"
    )
    JUVENILE = "Funding for juvenile court systems"
    NON_COURT_FUNCTIONS = (
        "Funding for non-court system functions such as law enforcement or jails"
    )
