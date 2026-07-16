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
"""Postgres I/O layer over the extractor-metadata tables."""

import datetime

from sqlalchemy.dialects.postgresql import insert

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.models.llm_extractor_config import (
    LLMExtractorConfig,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import LLMExtractorVersion


class LLMExtractorMetadataManager:
    """Postgres I/O over the extractor-metadata tables. Persists the current
    extractor version and associated info any time it changes.
    """

    def __init__(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)

    def set_active_extractor_version(
        self, *, config: LLMExtractorConfig
    ) -> LLMExtractorVersion:
        """Writes this extractor config to the DB if it is different than the
        current config written to the DB — upserting rows into the four
        config/version tables keyed on the computed version IDs, inserting a new
        llm_extractor_version row when extractor_version_id is new. Returns the
        active LLMExtractorVersion.
        """
        now = datetime.datetime.now(tz=datetime.UTC)
        collection = config.extractor_collection

        with SessionFactory.using_database(self.database_key) as session:
            self._assert_input_document_collection_unchanged(session, config=config)
            session.execute(
                insert(schema.LLMExtractorCollection)
                .values(
                    collection_name=collection.name,
                    collection_version_id=collection.collection_version_id,
                    output_schema_version=collection.output_schema_version,
                    output_schema_json=collection.generate_json_schema_str(),
                    description=collection.description,
                    minimum_confidence_level=collection.minimum_confidence_level.value,
                    row_creation_datetime_utc=now,
                )
                .on_conflict_do_nothing(constraint="llm_extractor_collection_pkey")
            )
            session.execute(
                insert(schema.LLMExtractor)
                .values(
                    state_code=config.state_code.value,
                    extractor_id=config.extractor_id,
                    extractor_collection_name=collection.name,
                    input_document_collection_name=config.input_document_collection.name,
                    row_creation_datetime_utc=now,
                )
                .on_conflict_do_nothing(constraint="llm_extractor_pkey")
            )
            session.execute(
                insert(schema.LLMExtractorDocumentFilter)
                .values(
                    state_code=config.state_code.value,
                    extractor_id=config.extractor_id,
                    document_filter_id=config.document_filter_id,
                    document_metadata_filter_query_template=config.document_metadata_filter_query_template,
                    row_creation_datetime_utc=now,
                )
                .on_conflict_do_nothing(constraint="llm_extractor_document_filter_pkey")
            )
            session.execute(
                insert(schema.LLMExtractorVersion)
                .values(
                    state_code=config.state_code.value,
                    extractor_version_id=config.extractor_version_id,
                    extractor_id=config.extractor_id,
                    extractor_collection_name=collection.name,
                    extractor_collection_version_id=collection.collection_version_id,
                    instructions_prompt=config.instructions_prompt,
                    instructions_prompt_hash=config.instructions_prompt_hash,
                    model_config_name=config.model_config.name,
                    model_config_version_id=config.model_config.model_config_version_id,
                    invalidated_datetime_utc=None,
                    invalidation_reason=None,
                    row_creation_datetime_utc=now,
                )
                .on_conflict_do_nothing(constraint="llm_extractor_version_pkey")
            )

            version = (
                session.query(schema.LLMExtractorVersion)
                .filter(
                    schema.LLMExtractorVersion.state_code == config.state_code.value,
                    schema.LLMExtractorVersion.extractor_version_id
                    == config.extractor_version_id,
                )
                .one()
            )
            return convert_schema_object_to_entity(
                version, LLMExtractorVersion, populate_direct_back_edges=False
            )

    def _assert_input_document_collection_unchanged(
        self, session: Session, *, config: LLMExtractorConfig
    ) -> None:
        """Raises if an llm_extractor row already exists for this extractor with a
        different input_document_collection_name. That value is invariant for a
        given extractor_id, and the on-conflict-do-nothing insert below would
        otherwise silently drop the change.
        """
        existing = (
            session.query(schema.LLMExtractor.input_document_collection_name)
            .filter(
                schema.LLMExtractor.state_code == config.state_code.value,
                schema.LLMExtractor.extractor_id == config.extractor_id,
            )
            .scalar()
        )
        if existing is not None and existing != config.input_document_collection.name:
            raise ValueError(
                f"Extractor [{config.extractor_id}] already reads input document "
                f"collection [{existing}], but config declares "
                f"[{config.input_document_collection.name}]. The input document "
                f"collection is invariant for an extractor_id; changing it "
                f"requires a new extractor_id."
            )

    def get_active_extractor_version(
        self, *, state_code: StateCode, collection_name: str
    ) -> LLMExtractorVersion:
        """Returns the current active version for the given (state_code,
        collection) — the most recent non-invalidated llm_extractor_version row.
        """
        with SessionFactory.using_database(self.database_key) as session:
            version = (
                session.query(schema.LLMExtractorVersion)
                .filter(
                    schema.LLMExtractorVersion.state_code == state_code.value,
                    schema.LLMExtractorVersion.extractor_collection_name
                    == collection_name,
                    schema.LLMExtractorVersion.invalidated_datetime_utc.is_(None),
                )
                .order_by(schema.LLMExtractorVersion.row_creation_datetime_utc.desc())
                .first()
            )
            if version is None:
                raise ValueError(
                    f"No active extractor version for state [{state_code.value}], "
                    f"collection [{collection_name}]."
                )
            return convert_schema_object_to_entity(
                version, LLMExtractorVersion, populate_direct_back_edges=False
            )
