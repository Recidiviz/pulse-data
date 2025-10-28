# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
# ============================================================================
"""Utils for categorizing decarceral impact"""
from enum import Enum
from typing import Dict


class DecarceralImpactType(Enum):
    """Represents an overarching type of decarceral impact (defined as a meaningful movement of
    a justice-impacted individual toward greater liberty), based on the theory of change of Recidiviz tools.
    """

    DOWNGRADE_CUSTODY_LEVEL = "DOWNGRADE_CUSTODY_LEVEL"
    DOWNGRADE_SUPERVISION_LEVEL = "DOWNGRADE_SUPERVISION_LEVEL"
    FURLOUGH = "FURLOUGH"
    # TODO(#50728): Update to reflect appropriate impact type for this completion event
    NO_DECARCERAL_IMPACT = "NO_DECARCERAL_IMPACT"
    RELEASE_TO_LIBERTY_FROM_SUPERVISION = "RELEASE_TO_LIBERTY_FROM_SUPERVISION"
    RELEASE_TO_PAROLE = "RELEASE_TO_PAROLE"
    SENTENCE_TO_PROBATION = "SENTENCE_TO_PROBATION"
    TRANSFER_OUT_OF_SOLITARY_CONFINEMENT = "TRANSFER_OUT_OF_SOLITARY_CONFINEMENT"
    TRANSFER_TO_COMMUNITY_CONFINEMENT = "TRANSFER_TO_COMMUNITY_CONFINEMENT"
    TRANSFER_TO_LIMITED_SUPERVISION = "TRANSFER_TO_LIMITED_SUPERVISION"
    TRANSFER_TO_NO_CONTACT_SUPERVISION = "TRANSFER_TO_NO_CONTACT_SUPERVISION"
    TRANSFER_TO_REENTRY_PREP_UNIT = "TRANSFER_TO_REENTRY_PREP_UNIT"
    TRANSFER_TO_WORK_RELEASE = "TRANSFER_TO_WORK_RELEASE"

    @classmethod
    def get_enum_description(cls) -> str:
        return "The overarching type of decarceral impact for a particular event."

    @classmethod
    def get_value_descriptions(cls) -> Dict["DecarceralImpactType", str]:
        return _DECARCERAL_IMPACT_TYPE_DESCRIPTIONS


_DECARCERAL_IMPACT_TYPE_DESCRIPTIONS: Dict[DecarceralImpactType, str] = {
    DecarceralImpactType.DOWNGRADE_CUSTODY_LEVEL: "Events related to a custody level downgrade"
    " as controlled by state-mandated assessment/classification levels.",
    DecarceralImpactType.DOWNGRADE_SUPERVISION_LEVEL: "Events related to a supervision level"
    " downgrade as controlled by state-mandated assessment/classification levels.",
    DecarceralImpactType.FURLOUGH: "Events related to a short-term release to community enabled"
    " by a temporary release policy (e.g., furlough).",
    DecarceralImpactType.NO_DECARCERAL_IMPACT: "Events related to transitions that do not"
    " have a decarceral impact.",
    DecarceralImpactType.RELEASE_TO_LIBERTY_FROM_SUPERVISION: "Events related to discharge from"
    " supervision to liberty.",
    DecarceralImpactType.RELEASE_TO_PAROLE: "Events related to release from incarceration"
    " to parole, excluding releases from temporary custody to parole (ie. parole reinstatement).",
    DecarceralImpactType.SENTENCE_TO_PROBATION: "Events related to a sentencing decision"
    " that places a person in probation. While the alternative to probation is not necessarily"
    " incarceration, a sentence to probation is on average a more decarceral option and one"
    " that is influenceable by our pre-sentencing tools.",
    DecarceralImpactType.TRANSFER_OUT_OF_SOLITARY_CONFINEMENT: "Events related to transitions"
    " out of solitary confinement/restrictive housing into a type of non-solitary/non-restrictive"
    " housing.",
    DecarceralImpactType.TRANSFER_TO_COMMUNITY_CONFINEMENT: "Events related to transitions from"
    " incarceration to a community or home confinement program outside of a DOC facility.",
    DecarceralImpactType.TRANSFER_TO_LIMITED_SUPERVISION: "Events related to transitions from"
    " standard supervision levels to an administrative supervision level that has limited"
    " mandatory reporting requirements between the client and supervising officer.",
    DecarceralImpactType.TRANSFER_TO_NO_CONTACT_SUPERVISION: "Events related to transitions from"
    " a supervision level with mandatory contacts to a supervision level with no regularly"
    " scheduled contacts between the client and supervising officer.",
    DecarceralImpactType.TRANSFER_TO_REENTRY_PREP_UNIT: "Events related to transitions into a"
    " DOC facility or facility unit exclusively for re-entry preparation.",
    DecarceralImpactType.TRANSFER_TO_WORK_RELEASE: "Events related to transitions to a work release"
    " facility, unit, or status. This event indicates when the work release privilege is granted, and"
    " not when someone is actually released temporarily.",
}
