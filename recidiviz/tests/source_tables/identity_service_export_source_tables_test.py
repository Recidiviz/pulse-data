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
"""Tests for build_identity_service_export_source_table_collection()"""

import unittest

from more_itertools import one

from recidiviz.persistence.database.schema.identity import schema
from recidiviz.source_tables.identity_service_export_source_tables import (
    IDENTITY_SERVICE_EXPORT_DATASET_ID,
    PERSON_OR_STAFF_ID_COLUMN,
    build_identity_service_export_source_table_collection,
)


class IdentityServiceSourceTablesTest(unittest.TestCase):
    """Tests for build_identity_service_export_source_table_collection()"""

    def test_collection_contains_expected_tables(self) -> None:
        collection = build_identity_service_export_source_table_collection()

        self.assertEqual(IDENTITY_SERVICE_EXPORT_DATASET_ID, collection.dataset_id)
        self.assertEqual(
            {
                "identities",
                "external_ids",
                "names",
                "dates_of_birth",
                "genders",
                "races",
                "sexes",
                "ethnicities",
                "phone_numbers",
                "emails",
            },
            {table.address.table_id for table in collection.source_tables},
        )

    def test_every_identity_table_is_classified(self) -> None:
        """Every table in the identity schema is either exported or explicitly
        listed here as operational, so adding a table to the schema forces a
        decision about whether identity data consumers should read it."""
        exported_table_ids = {
            table.address.table_id
            for table in build_identity_service_export_source_table_collection().source_tables
        }
        self.assertEqual(
            {
                "attribute_conflicts",
                "create_candidates",
                "merge_candidate_identities",
                "merge_candidates",
                "merge_events",
                "no_merge",
                "split_candidates",
                "split_event_moved_attributes",
                "split_event_moved_external_ids",
                "split_event_new_identities",
                "split_events",
                "update_attribute_candidates",
            },
            set(schema.IdentityBase.metadata.tables) - exported_table_ids,
        )

    def test_every_exported_column_is_documented(self) -> None:
        """Every exported column carries a description into BigQuery, sourced
        from the Postgres column comments (or built by hand for
        person_or_staff_id)."""
        undocumented = [
            f"{table.address.table_id}.{field.name}"
            for table in build_identity_service_export_source_table_collection().source_tables
            for field in table.schema_fields
            if not field.description
        ]
        self.assertEqual([], undocumented)

    def test_identities_schema(self) -> None:
        """The exported identities table drops the import-machinery columns,
        gains the computed person_or_staff_id column, and clusters by tenant."""
        collection = build_identity_service_export_source_table_collection()

        identities_table = one(
            table
            for table in collection.source_tables
            if table.address.table_id == "identities"
        )
        self.assertEqual(["tenant"], identities_table.clustering_fields)

        identities_field_names = [
            field.name for field in identities_table.schema_fields
        ]
        self.assertEqual(
            [
                "recidiviz_id",
                "created_utc",
                "last_updated_utc",
                "tenant",
                "person_type",
                "status",
                "merged_into",
                PERSON_OR_STAFF_ID_COLUMN,
            ],
            identities_field_names,
        )
