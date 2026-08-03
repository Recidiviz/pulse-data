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
"""Type aliases and value objects used by the identity ingest pipeline."""
import datetime

import attr

from recidiviz.common import attr_validators
from recidiviz.common.demographics import Ethnicity, Gender, Sex
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityFragment,
)
from recidiviz.pipelines.ingest.types import IngestViewName, UpperBoundDate

# An IdentityFragment paired with the upper-bound date and ingest view name it
# was sourced from.
SourcedIdentityFragment = tuple[UpperBoundDate, IngestViewName, IdentityFragment]

# A conflict-checked value in the form it is read off a fragment, before any
# serialization. Names and name suffixes are strings, birthdate is a date, and
# sex, gender, and ethnicity are their wrapped enums.
ConflictValue = str | datetime.date | Sex | Gender | Ethnicity


@attr.define(frozen=True, kw_only=True)
class AttributeConflict:
    """An attribute whose values conflict across a cluster's fragments, meaning
    the fragments likely do not describe one person."""

    field: str = attr.ib(validator=attr_validators.is_str)
    """Name of the attribute in conflict, e.g. "surname" or "birthdate"."""

    values: tuple[ConflictValue, ...] = attr.ib(
        validator=attr.validators.deep_iterable(
            member_validator=attr.validators.instance_of(
                (str, datetime.date, Sex, Gender, Ethnicity)
            ),
            iterable_validator=attr.validators.instance_of(tuple),
        )
    )
    """The distinct values recorded for the attribute across the cluster's
    fragments."""
