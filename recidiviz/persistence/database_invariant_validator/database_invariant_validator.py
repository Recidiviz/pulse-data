# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Functionality for validating that certain database invariants are true before we commit a transaction."""

import logging
from typing import Callable, List

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.schema.schema_person_type import SchemaPersonType
from recidiviz.persistence.database.schema.state.dao import SessionIsDirtyError
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database_invariant_validator.state.state_invariant_validators import (
    get_state_database_invariant_validators,
)

ValidatorType = Callable[[Session, str, List[SchemaPersonType]], bool]


def validate_invariants(
    session: Session,
    # TODO(#13703): Delete this param entirely.
    system_level: SystemLevel,
    region_code: str,
    output_people: List[SchemaPersonType],
) -> int:
    """Validates that certain database invariants are true for the given region.

    Args:
        session: The database session
        system_level: Denotes relevant schema we're ingesting for (always STATE)
        region_code: The string region code associated with this ingest job
        output_people: A list of all schema person objects touched by this session.
    """

    if system_level == SystemLevel.STATE:
        validators: List[ValidatorType] = get_state_database_invariant_validators()
    else:
        raise ValueError(f"Unexpected system level [{system_level}]")

    errors = 0
    for validator_fn in validators:
        try:
            success = validator_fn(session, region_code, output_people)
            if not success:
                errors += 1
        except SessionIsDirtyError as e:
            raise e
        except Exception as e:
            logging.error(
                "Exception checking database invariant [%s]: %s",
                validator_fn.__name__,
                e,
            )
            errors += 1

    return errors
