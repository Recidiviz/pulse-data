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
"""Tests for llm_extraction_eligible_document_query_builder.py, run against the
BQ emulator using the fake US_XX document collections."""

from pathlib import Path

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.llm_extraction_eligible_document_query_builder import (
    LLMExtractionEligibleDocumentQueryBuilder,
)
from recidiviz.documents.extraction.models.llm_extractor_config import (
    ExternalRootEntityNarrowing,
    InternalRootEntityNarrowing,
    LLMExtractorDocumentFilterConfig,
    RootEntityNarrowing,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    DocumentRootEntityIdType,
)
from recidiviz.documents.store.document_collection_config_collectors import (
    get_document_collection_config,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents import fake_config as fake_config_module
from recidiviz.tests.documents.extraction.fixtures import eligible_documents

# The id type every external-id fixture in this suite seeds its rows with.
_PERSON_EXTERNAL_ID_TYPE = "US_XX_DOC"
_STAFF_EXTERNAL_ID_TYPE = "US_XX_STAFF"


def _root_entity_narrowing(
    config: DocumentCollectionConfig, root_entity_ids: list[str] | None
) -> RootEntityNarrowing | None:
    """Returns the narrowing variant |config|'s root entity id type calls for,
    built from the string ids a test supplies.
    """
    if root_entity_ids is None:
        return None
    root_entity_id_type = config.root_entity_id_type
    if root_entity_id_type.id_type_column_name is None:
        return InternalRootEntityNarrowing(
            root_entity_ids=[int(root_id) for root_id in root_entity_ids]
        )
    return ExternalRootEntityNarrowing(
        root_entity_ids=root_entity_ids,
        external_id_type=(
            _PERSON_EXTERNAL_ID_TYPE
            if root_entity_id_type is DocumentRootEntityIdType.PERSON_EXTERNAL_ID
            else _STAFF_EXTERNAL_ID_TYPE
        ),
    )


class LLMExtractionEligibleDocumentQueryBuilderTest(BigQueryEmulatorTestCase):
    """Runs the eligible-document query against the emulator and asserts on the
    rows it selects."""

    def setUp(self) -> None:
        super().setUp()
        self.case_notes_config = self._config("FAKE_CASE_NOTES")

    @staticmethod
    def _config(collection_name: str) -> DocumentCollectionConfig:
        return get_document_collection_config(
            StateCode.US_XX, collection_name, fake_config_module
        )

    def _fixture_path(self, fixture_name: str) -> Path:
        return Path(eligible_documents.__file__).parent / fixture_name

    def _load_collection_tables(
        self,
        config: DocumentCollectionConfig,
        *,
        metadata_fixture: str,
        contents_fixture: str,
        source_sandbox_prefix: str | None = None,
    ) -> None:
        self.load_fixture_into_table(
            address=config.metadata_table_address(
                sandbox_dataset_prefix=source_sandbox_prefix
            ),
            schema=config.build_bq_metadata_schema(),
            fixture_path=self._fixture_path(metadata_fixture),
            fixture_columns=None,
            allow_comments=False,
        )
        self.load_fixture_into_table(
            address=config.document_contents_table_address(
                sandbox_dataset_prefix=source_sandbox_prefix
            ),
            schema=config.build_bq_document_contents_schema(),
            fixture_path=self._fixture_path(contents_fixture),
            fixture_columns=None,
            allow_comments=False,
        )

    def _run_query(
        self,
        config: DocumentCollectionConfig,
        *,
        document_limit: int | None = None,
        root_entity_ids: list[str] | None = None,
        filter_template: str | None = None,
        source_sandbox_prefix: str | None = None,
    ) -> str:
        # The production-shaped filter selects every live document in the
        # collection; individual tests override it to prove the filter narrows.
        default_filter = (
            "SELECT document_contents_id "
            "FROM `{input_document_collection_metadata_address}` "
            "WHERE document_contents_id IS NOT NULL"
        )
        narrowing = _root_entity_narrowing(config, root_entity_ids)
        is_narrowed = document_limit is not None or narrowing is not None
        return LLMExtractionEligibleDocumentQueryBuilder(
            document_filter=LLMExtractorDocumentFilterConfig(
                document_metadata_filter_query_template=(
                    filter_template if filter_template is not None else default_filter
                ),
                is_sandbox_config=is_narrowed,
                document_limit=document_limit,
                root_entity_narrowing=narrowing,
            ),
            input_document_collection=config,
            source_sandbox_prefix=source_sandbox_prefix,
        ).build_query(project_id=self.project_id)

    def _assert_matches_fixture(self, query: str, expected_fixture: str) -> None:
        self.compare_results_to_fixture(
            results=self.query(query),
            expected_output_fixture_path=self._fixture_path(expected_fixture),
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=True,
        )

    def _run_case_notes_query(
        self,
        *,
        document_limit: int | None = None,
        root_entity_ids: list[str] | None = None,
        filter_template: str | None = None,
    ) -> str:
        self._load_collection_tables(
            self.case_notes_config,
            metadata_fixture="case_notes_metadata_input.csv",
            contents_fixture="case_notes_contents_input.csv",
        )
        return self._run_query(
            self.case_notes_config,
            document_limit=document_limit,
            root_entity_ids=root_entity_ids,
            filter_template=filter_template,
        )

    def test_unnarrowed_selects_one_row_per_live_document(self) -> None:
        # Un-narrowed: one row per live document_contents_id with its
        # document_length_bytes and document_update_datetime. Covers:
        # - CID_HELLO: single note, returned as-is.
        # - CID_SHARED: identical text shared by P1/NOTE_2 (2026-03) and
        #   P2/NOTE_3 (2026-02); dedupes to the oldest (2026-02).
        # - CID_NEW: the latest metadata for P3/NOTE_4 supersedes CID_OLD, so
        #   CID_OLD never reaches latest_metadata and drops out.
        # - P4/NOTE_5: latest metadata has a null document_contents_id (deleted),
        #   excluded.
        self._assert_matches_fixture(
            self._run_case_notes_query(),
            "case_notes_unnarrowed_output.csv",
        )

    def test_sandbox_prefix_reads_from_prefixed_datasets(self) -> None:
        # With a source sandbox prefix set, the query reads the metadata and
        # contents from the prefixed sandbox datasets. The fixtures are loaded
        # only into the prefixed datasets, so a matching non-empty result proves
        # the prefix is threaded through to both addresses — an unthreaded prefix
        # would read the empty unprefixed datasets and return nothing.
        self._load_collection_tables(
            self.case_notes_config,
            metadata_fixture="case_notes_metadata_input.csv",
            contents_fixture="case_notes_contents_input.csv",
            source_sandbox_prefix="my_prefix",
        )
        self._assert_matches_fixture(
            self._run_query(self.case_notes_config, source_sandbox_prefix="my_prefix"),
            "case_notes_unnarrowed_output.csv",
        )

    def test_filter_narrows_to_selected_contents_ids(self) -> None:
        # Only the document_contents_ids the authored filter returns are eligible.
        self._assert_matches_fixture(
            self._run_case_notes_query(
                filter_template=(
                    "SELECT document_contents_id "
                    "FROM `{input_document_collection_metadata_address}` "
                    "WHERE document_contents_id = 'CID_HELLO'"
                )
            ),
            "case_notes_single_contents_id_output.csv",
        )

    def test_document_limit_caps_oldest_first(self) -> None:
        # The limit keeps the two oldest documents by document_update_datetime
        # (CID_HELLO and CID_SHARED), dropping the newer CID_NEW.
        self._assert_matches_fixture(
            self._run_case_notes_query(document_limit=2),
            "case_notes_limit_2_output.csv",
        )

    def test_root_entity_ids_narrows_to_matching_entities(self) -> None:
        # Narrowing to P1 keeps only P1's documents: CID_HELLO and P1's copy of
        # CID_SHARED. The root-entity WHERE runs before the dedupe QUALIFY, so
        # P2's older copy of CID_SHARED is filtered out first and P1's copy
        # (2026-03) is the one that survives — narrowing changes which copy of a
        # shared document surfaces.
        self._assert_matches_fixture(
            self._run_case_notes_query(root_entity_ids=["P1"]),
            "case_notes_narrowed_to_p1_output.csv",
        )

    def test_both_knobs_combine(self) -> None:
        # Root-entity narrowing runs first, then the oldest-first limit caps the
        # narrowed set to CID_HELLO.
        self._assert_matches_fixture(
            self._run_case_notes_query(document_limit=1, root_entity_ids=["P1"]),
            "case_notes_narrowed_to_p1_limit_1_output.csv",
        )

    def test_root_entity_ids_match_collection_root_column(self) -> None:
        # The narrowing predicate targets whichever root-entity ID column the
        # input collection uses, so each collection filters on its own column.
        # Each fixture seeds one document for the matched root entity (CID_KEEP)
        # and one for a different entity (CID_DROP) that the narrowing removes,
        # so all four collections resolve to the same expected output.
        for collection_name, metadata_fixture, root_id in [
            ("FAKE_CASE_NOTES", "person_external_id_metadata_input.csv", "P1"),
            ("FAKE_PERSON_ID_NOTES", "person_id_metadata_input.csv", "11"),
            ("FAKE_STAFF_ID_REPORTS", "staff_id_metadata_input.csv", "21"),
            ("FAKE_STAFF_REPORTS", "staff_external_id_metadata_input.csv", "S1"),
        ]:
            with self.subTest(collection_name=collection_name):
                config = self._config(collection_name)
                self._load_collection_tables(
                    config,
                    metadata_fixture=metadata_fixture,
                    contents_fixture="root_entity_contents_input.csv",
                )
                self._assert_matches_fixture(
                    self._run_query(config, root_entity_ids=[root_id]),
                    "root_entity_kept_output.csv",
                )

    def test_root_entity_ids_narrow_on_id_type_too(self) -> None:
        # The external id "P1" is only unambiguous together with its id type: here
        # it names one root entity under US_XX_DOC (CID_KEEP) and a different one
        # under US_XX_SID (CID_DROP). Narrowing to P1 of type US_XX_DOC keeps only
        # the former — matching on the id alone would pull in a different person's
        # document.
        config = self._config("FAKE_CASE_NOTES")
        self._load_collection_tables(
            config,
            metadata_fixture="shared_external_id_across_id_types_metadata_input.csv",
            contents_fixture="root_entity_contents_input.csv",
        )
        self._assert_matches_fixture(
            self._run_query(config, root_entity_ids=["P1"]),
            "root_entity_kept_output.csv",
        )

    def test_no_eligible_documents_returns_empty(self) -> None:
        self.assertTrue(
            self.query(
                self._run_case_notes_query(root_entity_ids=["NOT_A_PERSON"])
            ).empty
        )
