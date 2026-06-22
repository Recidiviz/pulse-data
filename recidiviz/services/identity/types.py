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
"""Domain types returned by IdentityServiceQuerier methods.

Kept deliberately separate from the ORM classes in
recidiviz/persistence/database/schema/identity/schema.py so the wire format
does not drift when internal pipeline-only columns are added or removed."""
import datetime
import uuid

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.identity import IdentityStatus, PersonType
from recidiviz.common.constants.tenants import Tenant


@attr.define(frozen=True, kw_only=True)
class Identity:
    """Recidiviz-assigned identity record."""

    def __attrs_post_init__(self) -> None:
        # Validate that `status` and `merged_into` fields are consistent.
        if self.status is IdentityStatus.RETIRED and self.merged_into is None:
            raise ValueError(
                f"Identity [{self.recidiviz_id}] has status RETIRED but no merged_into"
            )
        if self.merged_into is not None and self.status is not IdentityStatus.RETIRED:
            raise ValueError(
                f"Identity [{self.recidiviz_id}] has merged_into=[{self.merged_into}] "
                f"but status [{self.status}] is not RETIRED"
            )

    recidiviz_id: uuid.UUID = attr.ib(validator=attr.validators.instance_of(uuid.UUID))
    """Immutable Recidiviz-assigned id for this person."""

    tenant: Tenant = attr.ib(validator=attr.validators.instance_of(Tenant))
    """The jurisdiction or organization through which the person entered the
     system."""

    person_type: PersonType = attr.ib(validator=attr.validators.instance_of(PersonType))
    """Category of person this identity represents."""

    status: IdentityStatus = attr.ib(
        validator=attr.validators.instance_of(IdentityStatus)
    )
    """Lifecycle status: ACTIVE for active records, RETIRED after a merge."""

    merged_into: uuid.UUID | None = attr.ib(validator=attr_validators.is_opt(uuid.UUID))
    """The surviving Identity that absorbed this one. None for ACTIVE identities."""

    created_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When the Identity record was first created."""

    last_updated_utc: datetime.datetime = attr.ib(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )
    """When any field on this Identity or its child attributes was last modified."""
