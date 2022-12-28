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
    VACANT = "Clinical or medical staff positions budgeted but currently vacant"


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
    VACANT_CLINICAL_OR_MEDICAL = "Vacant clinical or medical staff positions"
    VACANT_PROGRAMMATIC = "Vacant programmatic staff positions"
    VACANT_UNKNOWN = "Vacant staff positions of unknown type"
    FILLED = "Filled positions"
