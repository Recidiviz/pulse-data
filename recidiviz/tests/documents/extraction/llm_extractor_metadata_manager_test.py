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
"""Tests for LLMExtractorMetadataManager against a real test Postgres."""

import unittest
from typing import Any

import attr
import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extractor_metadata_manager import (
    LLMExtractorMetadataManager,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
    get_llm_extractor_config,
)
from recidiviz.documents.extraction.models.llm_model_registry import (
    load_llm_model_registry,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.documents import fake_config
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_STATE_CODE = StateCode.US_XX
_COLLECTION_NAME = "FAKE_EXTRACTOR_COLLECTION"
# A first-order-valid model config in the fake registry distinct from the one the
# fake extractor resolves to (ACME_LARGE_DETERMINISTIC); used to evolve a config
# into one with a different extractor_version_id.
_OVERRIDE_MODEL_CONFIG_NAME = "ACME_LARGE_NO_THINKING"


@pytest.mark.uses_db
class LLMExtractorMetadataManagerTest(unittest.TestCase):
    """Tests for LLMExtractorMetadataManager."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.manager = LLMExtractorMetadataManager()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(
            self.manager.database_key.schema_type
        )
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.database_key
        )
        self.config = get_llm_extractor_config(
            _STATE_CODE, _COLLECTION_NAME, config_module=fake_config
        )

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def _count_rows(self, schema_class: Any) -> int:
        with SessionFactory.using_database(self.database_key) as session:
            return session.query(schema_class).count()

    def _config_with_different_version(self) -> LLMExtractorConfig:
        """Returns a config identical to self.config but bound to a different
        model config, which changes model_config_version_id and therefore
        extractor_version_id."""
        override_model_config = load_llm_model_registry(
            config_module=fake_config
        ).get_model_config(_OVERRIDE_MODEL_CONFIG_NAME)
        return attr.evolve(self.config, model_config=override_model_config)

    def test_set_active_extractor_version_on_empty_db(self) -> None:
        version = self.manager.set_active_extractor_version(config=self.config)

        # Exactly one row is written to each of the four config/version tables.
        self.assertEqual(1, self._count_rows(schema.LLMExtractorCollection))
        self.assertEqual(1, self._count_rows(schema.LLMExtractor))
        self.assertEqual(1, self._count_rows(schema.LLMExtractorDocumentFilter))
        self.assertEqual(1, self._count_rows(schema.LLMExtractorVersion))

        # The returned version carries the IDs computed off the config.
        self.assertEqual(_STATE_CODE.value, version.state_code)
        self.assertEqual(self.config.extractor_version_id, version.extractor_version_id)
        self.assertEqual(self.config.extractor_id, version.extractor_id)
        self.assertEqual(_COLLECTION_NAME, version.extractor_collection_name)
        self.assertEqual(
            self.config.extractor_collection.collection_version_id,
            version.extractor_collection_version_id,
        )
        self.assertEqual(self.config.model_config.name, version.model_config_name)
        self.assertEqual(
            self.config.model_config.model_config_version_id,
            version.model_config_version_id,
        )
        self.assertIsNone(version.invalidated_datetime_utc)

    def test_set_active_extractor_version_is_idempotent(self) -> None:
        first = self.manager.set_active_extractor_version(config=self.config)
        second = self.manager.set_active_extractor_version(config=self.config)

        # Re-writing the same config adds no new rows anywhere and returns the
        # same version.
        self.assertEqual(1, self._count_rows(schema.LLMExtractorCollection))
        self.assertEqual(1, self._count_rows(schema.LLMExtractor))
        self.assertEqual(1, self._count_rows(schema.LLMExtractorDocumentFilter))
        self.assertEqual(1, self._count_rows(schema.LLMExtractorVersion))
        self.assertEqual(first.extractor_version_id, second.extractor_version_id)
        self.assertEqual(
            first.row_creation_datetime_utc, second.row_creation_datetime_utc
        )

    def test_set_active_extractor_version_input_collection_change_raises(self) -> None:
        self.manager.set_active_extractor_version(config=self.config)
        changed_config = attr.evolve(
            self.config,
            input_document_collection=attr.evolve(
                self.config.input_document_collection, name="DIFFERENT_INPUT_NOTES"
            ),
        )
        with self.assertRaisesRegex(
            ValueError,
            r"already reads input document collection \[FAKE_INPUT_NOTES\], but "
            r"config declares \[DIFFERENT_INPUT_NOTES\]",
        ):
            self.manager.set_active_extractor_version(config=changed_config)

    def test_set_active_extractor_version_new_version(self) -> None:
        original = self.manager.set_active_extractor_version(config=self.config)
        changed_config = self._config_with_different_version()
        self.assertNotEqual(
            self.config.extractor_version_id, changed_config.extractor_version_id
        )

        updated = self.manager.set_active_extractor_version(config=changed_config)

        # A version-scoped change inserts a new version row (sharing the same
        # collection and extractor rows) and becomes the active version.
        self.assertEqual(2, self._count_rows(schema.LLMExtractorVersion))
        self.assertEqual(1, self._count_rows(schema.LLMExtractor))
        self.assertNotEqual(original.extractor_version_id, updated.extractor_version_id)
        active = self.manager.get_active_extractor_version(
            state_code=_STATE_CODE, collection_name=_COLLECTION_NAME
        )
        self.assertEqual(updated.extractor_version_id, active.extractor_version_id)

    def test_get_active_extractor_version_returns_written_row(self) -> None:
        written = self.manager.set_active_extractor_version(config=self.config)
        active = self.manager.get_active_extractor_version(
            state_code=_STATE_CODE, collection_name=_COLLECTION_NAME
        )
        self.assertEqual(written, active)

    def test_get_active_extractor_version_none_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"No active extractor version for state \[US_XX\]"
        ):
            self.manager.get_active_extractor_version(
                state_code=_STATE_CODE, collection_name=_COLLECTION_NAME
            )
