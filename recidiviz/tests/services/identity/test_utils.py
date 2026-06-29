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
"""Test helpers for the Identity Service."""
import datetime
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Iterator
from unittest.mock import patch

from recidiviz.common import demographics
from recidiviz.common.constants.identity import (
    AttributeType,
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    NameUse,
    PersonType,
    PhoneType,
    ProductApp,
    SourceType,
    SplitTrigger,
)
from recidiviz.common.constants.tenants import Tenant
from recidiviz.services.identity import types
from recidiviz.services.identity.constants import DEV_CALLER_SERVICE_ACCOUNT

TEST_PRODUCT_APP_BY_SERVICE_ACCOUNT: dict[str, ProductApp | None] = {
    DEV_CALLER_SERVICE_ACCOUNT: ProductApp.ADMIN_PANEL,
}

STRANGER_SERVICE_ACCOUNT = "stranger_service_account"
NO_APP_SERVICE_ACCOUNT = "no-app-caller_service_account"
ADMIN_PANEL_SERVICE_ACCOUNT = "admin-panel-caller_service_account"
MAPPED_SERVICE_ACCOUNT = "mapped_service_account"
DEFAULT_MAPPING = {MAPPED_SERVICE_ACCOUNT: ProductApp.ADMIN_PANEL}


@contextmanager
def mock_iap_environment(
    mapping: Mapping[str, ProductApp | None] | None = None,
    authenticated_as: str | None = None,
    invalid_jwt: bool = False,
    in_development: bool = False,
) -> Iterator[None]:
    """Patch the Identity Service IAP auth dependencies for the duration of the block.

    `mapping` replaces the static caller-to-source_product_app mapping.
    `authenticated_as` makes the JWT validator report that email as the
    authenticated caller. `invalid_jwt` makes the validator reject any header.
    `in_development` activates the decorator's dev bypass (no JWT validation)
    and the server's dev caller-email default.
    """
    if authenticated_as is not None and invalid_jwt:
        raise ValueError("authenticated_as and invalid_jwt are mutually exclusive")

    if invalid_jwt:
        jwt_return: tuple[str | None, str | None, str | None] = (
            None,
            None,
            "INVALID TOKEN",
        )
    else:
        jwt_return = ("sub-id", authenticated_as, None)

    with patch(
        "recidiviz.utils.auth.gce.in_development",
        return_value=in_development,
    ), patch(
        "recidiviz.services.identity.server.in_development",
        return_value=in_development,
    ), patch(
        "recidiviz.utils.metadata.project_number",
        return_value="123456789",
    ), patch(
        "recidiviz.utils.auth.gce.get_secret",
        return_value="987654321",
    ), patch(
        "recidiviz.utils.validate_jwt.validate_iap_jwt_from_compute_engine",
        return_value=jwt_return,
    ), patch.dict(
        "recidiviz.services.identity.helpers.PRODUCT_APP_BY_SERVICE_ACCOUNT",
        mapping or TEST_PRODUCT_APP_BY_SERVICE_ACCOUNT,
        clear=True,
    ):
        yield


# ---------------------------------------------------------------------------
# Domain object builders shared across the identity service test modules.
# ---------------------------------------------------------------------------

AttributeValue = (
    types.Name
    | types.DateOfBirth
    | types.Gender
    | types.Race
    | types.Sex
    | types.Ethnicity
    | types.PhoneNumber
    | types.Email
)

RECIDIVIZ_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
RETIRED_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
NEW_ID = uuid.UUID("33333333-3333-3333-3333-333333333333")
CREATED = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
UPDATED = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


def make_sourced_attribute(
    value: AttributeValue,
    *,
    source_type: SourceType = SourceType.EXTERNAL_DATA_SYSTEM,
    source_product_app: ProductApp | None = None,
    last_updated_utc: datetime.datetime = UPDATED,
) -> types.SourcedAttributeValue:
    """Wrap a value type in a SourcedAttributeValue with sensible defaults."""
    return types.SourcedAttributeValue(
        value=value,
        source_type=source_type,
        source_product_app=source_product_app,
        last_updated_utc=last_updated_utc,
    )


def build_full_identity() -> types.IdentityHistory:
    """A fully-populated IdentityHistory exercising every domain type."""
    identity = types.Identity(
        recidiviz_id=RECIDIVIZ_ID,
        tenant=Tenant.US_OZ,
        person_type=PersonType.JII,
        status=IdentityStatus.ACTIVE,
        merged_into=None,
        last_cluster_hash="cluster-hash-abc",
        skip_demographic_guard=False,
        created_utc=CREATED,
        last_updated_utc=UPDATED,
        external_ids=[
            types.ExternalId(
                external_id="A123", id_type=IdentifierType.US_OZ_LOTR_ID, is_active=True
            ),
            types.ExternalId(
                external_id="OLD9",
                id_type=IdentifierType.US_OZ_LOTR_ID,
                is_active=False,
            ),
        ],
        attributes=types.IdentityAttributes(
            names=[
                make_sourced_attribute(
                    types.Name(
                        surname="Baggins",
                        given_name="Frodo",
                        middle_names=["R"],
                        name_suffix=None,
                        use=NameUse.OFFICIAL,
                    )
                )
            ],
            dates_of_birth=[
                make_sourced_attribute(
                    types.DateOfBirth(
                        date=datetime.date(1990, 9, 22),
                        canonical=False,
                        canonical_locked=False,
                    )
                ),
                make_sourced_attribute(
                    types.DateOfBirth(
                        date=datetime.date(1990, 1, 1),
                        canonical=True,
                        canonical_locked=False,
                    ),
                    source_type=SourceType.PRODUCT_APP,
                    source_product_app=ProductApp.ADMIN_PANEL,
                ),
            ],
            genders=[
                make_sourced_attribute(
                    types.Gender(
                        gender=demographics.Gender.MALE,
                        canonical=True,
                        canonical_locked=False,
                    )
                )
            ],
            races=[make_sourced_attribute(types.Race(race=demographics.Race.WHITE))],
            sexes=[
                make_sourced_attribute(
                    types.Sex(
                        sex=demographics.Sex.MALE,
                        canonical=True,
                        canonical_locked=False,
                    )
                )
            ],
            ethnicities=[
                make_sourced_attribute(
                    types.Ethnicity(
                        ethnicity=demographics.Ethnicity.NOT_HISPANIC,
                        canonical=True,
                        canonical_locked=False,
                    )
                )
            ],
            phone_numbers=[
                make_sourced_attribute(
                    types.PhoneNumber(
                        number="5551234567", type=PhoneType.CELL, preferred=True
                    )
                )
            ],
            emails=[
                make_sourced_attribute(
                    types.Email(address="test@fake.com", address_hash="hashtestfakecom")
                )
            ],
        ),
    )
    return types.IdentityHistory(
        identity=identity,
        merge_events=[
            types.MergeEvent(
                surviving_id=RECIDIVIZ_ID,
                retired_id=RETIRED_ID,
                trigger=MergeTrigger.MERGE_ENDPOINT,
                requested_by="auditor@fake.com",
                timestamp_utc=UPDATED,
                conflicts=[
                    types.AttributeConflict(
                        attribute_type=AttributeType.NAME,
                        retired_value=make_sourced_attribute(
                            types.Name(
                                surname="Baggins",
                                given_name="Frodo",
                                middle_names=[],
                                name_suffix=None,
                                use=NameUse.OFFICIAL,
                            )
                        ),
                        surviving_value=make_sourced_attribute(
                            types.Name(
                                surname="Baggins",
                                given_name="Mr. Frodo",
                                middle_names=[],
                                name_suffix=None,
                                use=NameUse.OFFICIAL,
                            )
                        ),
                    )
                ],
            )
        ],
        split_events=[
            types.SplitEvent(
                original_id=RECIDIVIZ_ID,
                trigger=SplitTrigger.SPLIT_ENDPOINT,
                requested_by=None,
                timestamp_utc=UPDATED,
                destinations=[
                    types.SplitDestination(
                        new_recidiviz_id=NEW_ID,
                        external_ids=[
                            types.ExternalId(
                                external_id="OLD9",
                                id_type=IdentifierType.US_OZ_LOTR_ID,
                                is_active=True,
                            )
                        ],
                        attributes=[
                            make_sourced_attribute(
                                types.Email(
                                    address="test@fake.com",
                                    address_hash="hashtestfakecom",
                                )
                            )
                        ],
                    )
                ],
            )
        ],
    )
