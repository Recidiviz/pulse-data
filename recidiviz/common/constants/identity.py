# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Enums and constants for the Recidiviz Identity System."""
import enum

from recidiviz.common.constants.state import external_id_types


class IdentifierType(enum.Enum):
    """Type of external identifier stored on an external_ids row.

    Stored as a String in the DB (see StringBackedEnum) so adding an identifier
    type is a code change rather than a migration. Values mirror the canonical
    constants in recidiviz/common/constants/state/external_id_types.py.
    """

    # TODO(#81647): Populate this with real identifier types
    US_OZ_LOTR_ID = external_id_types.US_OZ_LOTR_ID
    US_OZ_KDS_PERSON_ID = external_id_types.US_OZ_KDS_PERSON_ID
    US_OZ_IDENTITY_PERSON_ID = external_id_types.US_OZ_IDENTITY_PERSON_ID
    US_OZ_IDENTITY_STAFF_ID = external_id_types.US_OZ_IDENTITY_STAFF_ID
    US_OZ_IDENTITY_BADGE_ID = external_id_types.US_OZ_IDENTITY_BADGE_ID


class PersonType(enum.Enum):
    """The type of person tracked in Recidiviz's identity system."""

    JII = "JII"
    """Justice-involved individual."""

    STAFF = "STAFF"
    """Staff member at a justice agency."""

    RECIDIVIZ_EMPLOYEE = "RECIDIVIZ_EMPLOYEE"
    """Recidiviz employee."""


class IdentityStatus(enum.Enum):
    """Lifecycle status of an identity record."""

    ACTIVE = "ACTIVE"
    """Currently live identity record."""

    RETIRED = "RETIRED"
    """Identity that has been merged into another. The `merged_into` column
    points to the surviving record."""


class NameUse(enum.Enum):
    """How a name attribute is used or designated by its source."""

    OFFICIAL = "OFFICIAL"
    """Legal/official name as recorded by the source system."""

    PREFERRED = "PREFERRED"
    """Name the person prefers to be called."""

    FORMER = "FORMER"
    """A prior name no longer used (e.g., a maiden name)."""

    ALIAS = "ALIAS"
    """An alternate or known-as name."""


class SourceType(enum.Enum):
    """Provenance of an attribute value."""

    EXTERNAL_DATA_SYSTEM = "EXTERNAL_DATA_SYSTEM"
    """Originated from a state partner or other external data system import."""

    PRODUCT_APP = "PRODUCT_APP"
    """Set or modified by a Recidiviz product application."""

    ADMIN_OVERRIDE = "ADMIN_OVERRIDE"
    """Set or modified by a Recidiviz administrator via a manual override path."""


class PhoneType(enum.Enum):
    """Category of phone number."""

    CELL = "CELL"
    HOME = "HOME"
    WORK = "WORK"
    OTHER = "OTHER"


class ProductApp(enum.Enum):
    """Product app that sourced a PRODUCT_APP-typed attribute value."""

    ADMIN_PANEL = "admin_panel"


class AttributeType(enum.Enum):
    """Which attribute table a stored attribute snapshot refers to.

    Used by audit tables (attribute_conflicts, split_event_moved_attributes)
    that store attribute snapshots as JSONB and need to record which kind of
    attribute the snapshot represents.
    """

    NAME = "NAME"
    DATE_OF_BIRTH = "DATE_OF_BIRTH"
    GENDER = "GENDER"
    RACE = "RACE"
    SEX = "SEX"
    ETHNICITY = "ETHNICITY"
    PHONE_NUMBER = "PHONE_NUMBER"
    EMAIL = "EMAIL"


class CandidateStatus(enum.Enum):
    """Review status of a candidate row that can become invalidated."""

    PENDING = "PENDING"
    """Awaiting human review."""

    RESOLVED = "RESOLVED"
    """A reviewer has acted on the candidate."""

    STALE = "STALE"
    """Other writes have changed the underlying identity state such that this
    candidate no longer reflects an actionable proposal."""


class CreateCandidateStatus(enum.Enum):
    """Review status of a create_candidates row.

    Create candidates have no STALE state because they are deleted if they become invalid.
    """

    PENDING = "PENDING"
    """Awaiting human review."""

    RESOLVED = "RESOLVED"
    """A reviewer has acted on the candidate."""


class MergeTrigger(enum.Enum):
    """What initiated a merge event."""

    IMPORT = "IMPORT"
    """Merge triggered automatically by the POST /import endpoint while reconciling clustering results."""

    MERGE_ENDPOINT = "MERGE_ENDPOINT"
    """Merge triggered by an explicit call to the merge API."""


class SplitTrigger(enum.Enum):
    """What initiated a split event."""

    IMPORT = "IMPORT"
    """Split triggered automatically by the POST /import endpoint while reconciling clustering results."""

    SPLIT_ENDPOINT = "SPLIT_ENDPOINT"
    """Split triggered by an explicit call to the split API."""
