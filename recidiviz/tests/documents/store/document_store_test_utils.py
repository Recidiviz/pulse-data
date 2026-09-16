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
"""Shared helpers for document-store tests: loaders for the fake US_XX document
collection."""
from contextlib import AbstractContextManager
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    collect_document_collection_config_yaml_paths,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.tests.documents import fake_config

FAKE_INPUT_DOCUMENT_COLLECTION_NAME = "FAKE_INPUT_NOTES"


def get_fake_first_order_document_collection_config() -> DocumentCollectionConfig:
    """Returns the fake US_XX first-order document collection config."""
    return get_document_collection_config(
        StateCode.US_XX,
        FAKE_INPUT_DOCUMENT_COLLECTION_NAME,
        config_module=fake_config,
    )


def patch_fake_config_states_exist_in_env() -> AbstractContextManager[MagicMock]:
    """Returns a patch that treats the fake config module's document-collection
    states as existing in the environment. Enter it around any code that collects
    document-store source tables from the fake config module.
    """
    fake_config_states = [
        sc
        for sc in StateCode
        if collect_document_collection_config_yaml_paths(sc, config_module=fake_config)
    ]
    return patch(
        "recidiviz.documents.store.document_collection_config."
        "get_direct_ingest_states_existing_in_env",
        MagicMock(return_value=fake_config_states),
    )
