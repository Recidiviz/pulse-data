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
from typing import Callable, List, Type

from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema.state.dao import SessionIsDirtyError
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database_invariant_validator.state.state_person_invariant_validators import (
    get_state_person_database_invariant_validators,
)
from recidiviz.persistence.database_invariant_validator.state.state_staff_invariant_validators import (
    get_state_staff_database_invariant_validators,
)
from recidiviz.persistence.persistence_utils import SchemaRootEntityT

ValidatorType = Callable[[Session, str, List[state_schema.StatePerson]], bool]


def validate_invariants(
    session: Session,
    region_code: str,
    schema_root_entity_cls: Type[SchemaRootEntityT],
    output_root_entities: List[SchemaRootEntityT],
) -> int:
    """Validates that certain database invariants are true for the given region.

    Args:
        session: The database session
        region_code: The string region code associated with this ingest job
        schema_root_entity_cls: The type of the objects in |output_root_entities|.
        output_root_entities: A list of all schema person objects touched by this session.
    """

    validators: List[Callable[[Session, str, List[SchemaRootEntityT]], bool]]
    if schema_root_entity_cls is state_schema.StatePerson:
        validators = get_state_person_database_invariant_validators()
    elif schema_root_entity_cls is state_schema.StateStaff:
        validators = get_state_staff_database_invariant_validators()
    else:
        raise ValueError(f"Unexpected root entity class [{schema_root_entity_cls}]")

    errors = 0
    for validator_fn in validators:
        try:
            success = validator_fn(session, region_code, output_root_entities)
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
