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
"""Includes/Excludes definitions that are shared across agencies """


import enum


# TODO(#18071)
class FelonyCasesIncludesExcludes(enum.Enum):
    FELONY = "Cases with a leading felony charge"
    MISDEMEANOR = "Cases with a leading misdemeanor or infraction charge"


class MisdemeanorCasesIncludesExcludes(enum.Enum):
    MISDEMEANOR = "Cases with a leading misdemeanor charge"
    FELONY = "Cases with a leading felony charge"
    INFRACTION = "Cases with a leading infraction charge"


class StateAppropriationIncludesExcludes(enum.Enum):
    FINALIZED = "Finalized state appropriations"
    PROPOSED = "Proposed state appropriations"
    PRELIMINARY = "Preliminary state appropriations"
    GRANTS = "Grants from state sources that are not budget appropriations approved by the legislature/governor"


class CountyOrMunicipalAppropriationIncludesExcludes(enum.Enum):
    FINALIZED = "Finalized county or municipal appropriations"
    PROPOSED = "Proposed county or municipal appropriations"
    PRELIMINARY = "Preliminary county or municipal appropriations"


class GrantsIncludesExcludes(enum.Enum):
    LOCAL = "Local grants"
    STATE = "State grants"
    FEDERAL = "Federal grants"
    PRIVATE = "Private or foundation grants"


class StaffIncludesExcludes(enum.Enum):
    FILLED = "Filled positions"
    VACANT = "Staff positions budgeted but currently vacant"
    FULL_TIME = "Full-time positions"
    PART_TIME = "Part-time positions"
    CONTRACTED = "Contracted positions"
    TEMPORARY = "Temporary positions"
    VOLUNTEER = "Volunteer positions"
    INTERN = "Intern positions"


class CasesDismissedIncludesExcludes(enum.Enum):
    FELONY = "Cases with a leading felony charge dismissed"
    MISDEMEANOR = "Cases with a leading misdemeanor charge dismissed"


class CasesResolvedByPleaIncludesExcludes(enum.Enum):
    FELONY = "Cases with a leading felony charge resolved by plea"
    MISDEMEANOR = "Cases with a leading misdemeanor charge resolved by plea"


class CasesResolvedAtTrialIncludesExcludes(enum.Enum):
    FELONY = "Cases with a leading felony charge resolved at trial"
    MISDEMEANOR = "Cases with a leading misdemeanor charge resolved at trial"


class FacilitiesAndEquipmentExpensesIncludesExcludes(enum.Enum):
    OPERATIONS = "Facility operations"
    MAINTENANCE = "Facility maintenance"
    RENOVATION = "Facility renovation"
    CONSTRUCTION = "Facility construction"
    TECHNOLOGY = "Equipment (e.g., computers, communication, and information technology infrastructure)"


class PreAdjudicationJailPopulation(enum.Enum):
    AWAITING_ARRAIGNMENT = "People in jail awaiting arraignment"
    UNPAID_BAIL = "People in jail due to unpaid bail"
    DENIAL_OF_BAIL = "People in jail due to denial of bail"
    REVOCATION_OF_BAIL = "People in jail due to revocation of bail"
    PENDING_ASSESSMENT = "People in jail pending assessment of capacity to stand trial"
    TRANSFERRED_TO_HOSPITAL = "People who have been transferred to a hospital for a capacity assessment but are still counted in jail population"
    PENDING_PRETRIAL_OUTCOME = (
        "People in jail to be held pending outcome of pretrial revocation decision"
    )
    REVOCATION_PRETRIAL_RELEASE = "People in jail due to revocation of pretrial release"
    PRETRIAL_SUPERVISION_SANCTION = (
        "People in jail due to a pretrial supervision incarceration sanction"
    )
    US_MARSHALS_SERVICE = "People in jail due to a pre-adjudication federal hold for U.S. Marshals Service, Federal Bureau of Prisons, or U.S. Immigration and Customs Enforcement"
    TRIBAL_NATION = "People in jail due to a pre-adjudication federal hold for a Tribal Nation or the Bureau of Indian Affairs"
    FAILURE_TO_APPEAR = "People held awaiting hearings for failure to appear in court or court-ordered programs"
    FAILURE_TO_PAY = "People held due to failure to pay fines or fees ordered by civil or criminal courts"
    HELD_FOR_OTHER_STATE = "People held for other state or county jurisdictions"
    SERVE_SENTENCE = "People in jail to serve a sentence of jail incarceration"
    SPLIT_SENTENCE = "People in jail to serve a split sentence of jail incarceration"
    SUSPEND_SENTENCE = (
        "People in jail to serve a suspended sentence of jail incarceration"
    )
    REVOCATION_COMMUNITY_SUPERVISION = "People in jail due to a revocation of post-adjudication community supervision sentence (i.e., probation, parole, or other community supervision sentence type)"
    COMMUNITY_SUPERVISION_SANCTION = "People in jail due to a post-adjudication incarceration sanction imposed by a community supervision agency (e.g., a “dip,” “dunk,” or weekend sentence)"
    COURT_SANCTION = "People in jail due to a post-adjudication incarceration sanction imposed by a specialty, treatment, or problem-solving court (e.g., a “dip,” “dunk,” or weekend sentence)"


class PostAdjudicationJailPopulation(enum.Enum):
    JAIL_INCARCERATION = "People in jail to serve a sentence of jail incarceration"
    PRISON_SENTENCE = "People in jail to serve a state prison sentence"
    SPLIT_SENTENCE = "People in jail to serve a split sentence of jail incarceration"
    SUSPENDED_SENTENCE = (
        "People in jail to serve a suspended sentence of jail incarceration"
    )
    REVOCATION_COMMUNITY_SUPERVISION = "People in jail due to a revocation of post-adjudication community supervision sentence (i.e., probation, parole, or other community supervision sentence type)"
    COMMUNITY_SUPERVISION_SANCTION = "People in jail due to a post-adjudication incarceration sanction imposed by a community supervision agency (e.g., a “dip,” “dunk,” or weekend sentence)"
    COURT_SANCTION = "People in jail due to a post-adjudication incarceration sanction imposed by a specialty, treatment, or problem-solving court (e.g., a “dip,” “dunk,” or weekend sentence)"
    AWAITING_ARRAIGNMENT = "People in jail awaiting arraignment"
    UNPAID_BAIL = "People in jail due to unpaid bail"
    DENIAL_OF_BAIL = "People in jail due to denial of bail"
    REVOCATION_OF_BAIL = "People in jail due to revocation of bail"
    PENDING_ASSESSMENT = "People in jail pending assessment of capacity to stand trial"
    TRANSFERRED_TO_HOSPITAL = "People who have been transferred to a hospital for a capacity assessment but are still counted on jail rolls"
    PENDING_OUTCOME = (
        "People in jail to be held pending outcome of pretrial revocation decision"
    )
    REVOCATION_PRETRIAL_RELEASE = "People in jail due to revocation of pretrial release"
    PRETRIAL_SUPERVISION_SANCTION = (
        "People in jail due to a pretrial supervision incarceration sanction"
    )
    US_MARSHALS_SERVICE = "People in jail due to a pre-adjudication federal hold for U.S. Marshals Service, Federal Bureau of Prisons, or U.S. Immigration and Customs Enforcement"
    TRIBAL_NATION = "People in jail due to a pre-adjudication federal hold for a Tribal Nation or the Bureau of Indian Affairs"
    FAILURE_TO_APPEAR = "People held awaiting hearings for failure to appear in court or court-ordered programs"
    FAILURE_TO_PAY = "People held due to failure to pay fines or fees ordered by civil or criminal courts"
    HELD_FOR_OTHER_STATE = "People held for other state or county jurisdictions"


class ProbationDefinitionIncludesExcludes(enum.Enum):
    PROBATION_IN_LIEU_INCARCERATION = "People sentenced to a period of probation in lieu of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    PROBATION_AFTER_INCARCERATION = "People sentenced to a period of probation after a period of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    PROBATION_POST_ADJUCATION_PROGRAM = "People on probation as part of a post-adjudication specialty or problem-solving court program (e.g., drug court)"
    PROBATION_TEMPORARILY_CONFINED = "People sentenced to probation who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    PROBATION_CONFINED_ANY_LENGTH = "People sentenced to probation confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    PROBATION_HOLD_PENDING = "People sentenced to probation who are in jail or prison on a hold pending resolution of a violation or revocation"
    PROBATION_LONGER_SANCTION = "People sentenced to probation who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    PROBATION_COMPACT_AGREEMENT = "People sentenced to probation in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    PROBATION_ANOTHER_JURISTICTION = (
        "People sentenced to probation who are being supervised by another jurisdiction"
    )
    PROBATION_IN_COMMUNITY = "People who have not been sentenced but are supervised on probation in the community prior to the resolution of their case"
    PROBATION_ANOTHER_FORM_SUPERVISION = (
        "People sentenced to probation who are also on another form of supervision"
    )
    PROBATION_PRE_ADJUCTATION_PROGRAM = "People on probation as part of a pre-adjudication specialty or problem-solving court program (e.g., drug court)"


class ParoleDefinitionIncludesExcludes(enum.Enum):
    PAROLE_EARLY_RELEASE = "People approved by a parole board or similar entity for early conditional release from incarceration to parole supervision (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    PAROLE_STATUTORY_REQUIREMENT = "People conditionally released from incarceration to parole supervision by statutory requirement (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    PAROLE_TEMPORARILY_CONFINED = "People on parole who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    PAROLE_CONFINED_ANY_LENGTH = "People on parole confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    PAROLE_HOLD_PENDING = "People on parole who are in jail or prison on a hold pending resolution of a violation or revocation"
    PAROLE_LONGER_SANCTION = "People on parole who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    PAROLE_COMPACT_AGREEMENT = "People released to parole in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    PAROLE_ANOTHER_FORM_SUPERVISION = (
        "People on parole who are also on another form of supervision"
    )
    PAROLE_ANOTHER_JURISTICTION = (
        "People on parole who are being supervised by another jurisdiction"
    )


class PretrialDefinitionIncludesExcludes(enum.Enum):
    PRETRIAL_CITATION_RELEASE = "People on citation release (i.e., were never booked)"
    PRETRIAL_CONDITION_SUPERVISION = "People released from jail or otherwise not held pretrial on the condition of supervision (including electronic monitoring, home confinement, traditional supervision, etc.)"
    PRETRIAL_STATUTORY_REQUIREMENT = "People released from jail or otherwise not held pretrial due to statutory requirement"
    PRETRIAL_COURT_PROGRAM = "People supervised as part of a pre-adjudication specialty or problem-solving court program (e.g., drug court)"
    PRETRIAL_HOLD_PENDING = "People on pretrial supervision who are incarcerated on a hold pending resolution of a violation or revocation"
    PRETRIAL_ANOTHER_FORM_SUPERVISION = (
        "People on pretrial supervision who are also on another form of supervision"
    )


class OtherCommunityDefinitionIncludesExcludes(enum.Enum):
    OTHER_IN_LIEU_INCARCERATION = "People sentenced to a period of other community supervision in lieu of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    OTHER_DETERMINATE_PERIOD = "People sentenced to a determinate period of other community supervision after a period of incarceration (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    OTHER_POST_ADJUCATION_PROGRAM = "People on other community supervision as part of a post-adjudication specialty or problem-solving court program (e.g., drug court)"
    OTHER_EARLY_RELEASE = "People approved by a parole board or similar entity for early conditional release from incarceration to other community supervision (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    OTHER_STATUTORY_REQUIREMENT = "People conditionally released from incarceration to other community supervision by statutory requirement (including to electronic monitoring, home confinement, traditional supervision, etc.)"
    OTHER_TEMPORARILY_CONFINED = "People on other community supervision who are temporarily confined in jail, prison, or another confinement center for a short “dip” sanction (typically less than 30 days)"
    OTHER_CONFINED_ANY_LENGTH = "People on other community supervision confined for any length of time in a violation center or halfway back facility operated by the supervision agency"
    OTHER_JAIL_OR_PRISON_HOLD_PENDING = "People on other community supervision who are in jail or prison on a hold pending resolution of a violation or revocation"
    OTHER_LONGER_SANTION = "People on other community supervision who are confined in jail or prison for a longer sanction (e.g., more than 30 days, 120 days, 6 months, etc.)"
    OTHER_INCARCERATED_HOLD_PENDING = "People on other community supervision who are incarcerated on a hold pending resolution of a violation or revocation"
    OTHER_COMPACT_AGREEMENT = "People on supervision in another jurisdiction who are supervised by the agency through interstate compact, intercounty compact, or other mutual supervision agreement"
    OTHER_ANOTHER_FORM_SUPERVISION = "People on other community supervision who are also on another form of supervision"
    OTHER_PRIOR_TO_RESOLUTION = "People on other community supervision who have not been sentenced but are supervised in the community prior to the resolution of their case"
    OTHER_COURT_PROGRAM = "People on other community supervision in a pre-adjudication specialty or problem-solving court program (e.g., drug court, etc.)"


# Caseload


class CaseloadNumeratorIncludesExcludes(enum.Enum):
    OPEN = "Criminal cases open and active during the time period"
    ASSIGNED = "Criminal cases assigned to an attorney but inactive"
    NOT_ASSIGNED = "Criminal cases not yet assigned to an attorney"


class FelonyCaseloadNumeratorIncludesExcludes(enum.Enum):
    OPEN_CASES = "Felony cases open and active during the sharing period"
    ASSIGNED_CASES = "Felony cases assigned to an attorney but inactive"
    UNASSIGNED_CASES = "Felony cases not yet assigned to an attorney"


class MisdemeanorCaseloadNumeratorIncludesExcludes(enum.Enum):
    OPEN_CASES = "Misdemeanor cases open and active during the sharing period"
    ASSIGNED_CASES = "Misdemeanor cases assigned to an attorney but inactive"
    UNASSIGNED_CASES = "Misdemeanor cases not yet assigned to an attorney"


class MixedCaseloadNumeratorIncludesExcludes(enum.Enum):
    OPEN_FELONY_CASES = "Felony cases open and active during the sharing period"
    ASSIGNED_FELONY_CASES = "Felony cases assigned to an attorney but inactive"
    OPEN_MISDEMEANOR_CASES = (
        "Misdemeanor cases open and active during the sharing period"
    )
    ASSIGNED_MISDEMEANOR_CASES = (
        "Misdemeanor cases assigned to an attorney but inactive"
    )
    UNASSIGNED_FELONY_CASES = "Felony cases not yet assigned to an attorney"
    UNASSIGNED_MISDEMEANOR_CASES = "Misdemeanor cases not yet assigned to an attorney"
