# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# pylint: disable=protected-access
"""Base test class for testing subclasses of BaseHistoricalSnapshotUpdater"""
import datetime
from types import ModuleType
from typing import List, Set
from unittest import TestCase

from more_itertools import one

from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.history.historical_snapshot_update import (
    update_historical_snapshots,
)
from recidiviz.persistence.database.schema.county.history_table_shared_columns_mixin import (
    HistoryTableSharedColumns,
)
from recidiviz.persistence.database.schema.schema_person_type import SchemaPersonType
from recidiviz.persistence.database.schema_utils import (
    HISTORICAL_TABLE_CLASS_SUFFIX,
    _get_all_database_entities_in_module,
    historical_table_class_from_obj,
    schema_type_for_object,
    schema_type_for_schema_module,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.core_entity import (
    primary_key_name_from_obj,
    primary_key_value_from_obj,
)


class BaseHistoricalSnapshotUpdaterTest(TestCase):
    """
    Base test class for testing subclasses of BaseHistoricalSnapshotUpdater
    """

    @staticmethod
    def _commit_person(
        person: SchemaPersonType,
        system_level: SystemLevel,
        ingest_time: datetime.datetime,
    ):
        db_key = SQLAlchemyDatabaseKey.canonical_for_schema(system_level.schema_type())
        with SessionFactory.using_database(db_key) as act_session:
            merged_person = act_session.merge(person)

            metadata = IngestMetadata(
                region="somewhere",
                ingest_time=ingest_time,
                system_level=system_level,
                database_key=db_key,
            )
            update_historical_snapshots(act_session, [merged_person], [], metadata)

    def _check_all_non_history_schema_object_types_in_list(
        self,
        schema_objects: List[DatabaseEntity],
        schema: ModuleType,
        schema_object_type_names_to_ignore: List[str],
    ) -> None:
        expected_schema_object_types = (
            self._get_all_non_history_database_entity_type_names_in_module(schema)
        )

        expected_schema_object_types = expected_schema_object_types.difference(
            set(schema_object_type_names_to_ignore)
        )
        actual_schema_object_types = {obj.__class__.__name__ for obj in schema_objects}

        self.assertEqual(expected_schema_object_types, actual_schema_object_types)

    @staticmethod
    def _get_all_non_history_database_entity_type_names_in_module(
        schema: ModuleType,
    ) -> Set[str]:
        all_table_classes = _get_all_database_entities_in_module(schema)
        expected_schema_object_types: Set[str] = set()
        for table_class in all_table_classes:
            class_name = table_class.__name__
            if not class_name.endswith(HISTORICAL_TABLE_CLASS_SUFFIX):
                expected_schema_object_types.add(class_name)

        return expected_schema_object_types

    def _get_all_schema_objects_in_db(
        self,
        schema_person_type: SchemaPersonType,
        schema: ModuleType,
        schema_object_type_names_to_ignore: List[str],
    ) -> List[DatabaseEntity]:
        """Generates a list of all schema objects stored in the database that
        can be reached from an object with the provided type.

        Args:
            schema_person_type: Class type of the root of the schema object
                graph (e.g. StatePerson).
            schema: The schema module that root_object_type is defined in.
            schema_object_type_names_to_ignore: type names for objects defined
                in the schema that we shouldn't assert are included in the
                object graph.

        Returns:
            A list of all schema objects that can be reached from the object
            graph rooted at the singular object of type |schema_person_type|.

        Throws:
            If more than one object of type |schema_person_type| exists in the
            DB.
        """

        with SessionFactory.using_database(
            SQLAlchemyDatabaseKey.canonical_for_schema(
                schema_type_for_schema_module(schema)
            ),
            autocommit=False,
        ) as session:
            person = one(session.query(schema_person_type).all())

            schema_objects: Set[DatabaseEntity] = {person}
            unprocessed = list([person])
            while unprocessed:
                schema_object = unprocessed.pop()

                related_entities = []
                for (
                    relationship_name
                ) in schema_object.get_relationship_property_names():
                    related = getattr(schema_object, relationship_name)

                    # Relationship can return either a list or a single item
                    if isinstance(related, DatabaseEntity):
                        related_entities.append(related)
                    if isinstance(related, list):
                        related_entities.extend(related)

                    for obj in related_entities:
                        if obj not in schema_objects:
                            schema_objects.add(obj)
                            unprocessed.append(obj)

        self._check_all_non_history_schema_object_types_in_list(
            list(schema_objects), schema, schema_object_type_names_to_ignore
        )

        return list(schema_objects)

    def _assert_expected_snapshots_for_schema_object(
        self, expected_schema_object: DatabaseEntity, ingest_times: List[datetime.date]
    ) -> None:
        """
        Assert that we have expected history snapshots for the given schema
        object that has been ingested at the provided |ingest_times|.
        """
        history_table_class = historical_table_class_from_obj(expected_schema_object)
        self.assertIsNotNone(history_table_class)

        schema_obj_primary_key_col_name = primary_key_name_from_obj(
            expected_schema_object
        )
        schema_obj_primary_key_value = primary_key_value_from_obj(
            expected_schema_object
        )

        self.assertIsNotNone(schema_obj_primary_key_value)
        self.assertEqual(type(schema_obj_primary_key_value), int)

        schema_obj_foreign_key_column_in_history_table = getattr(
            history_table_class, schema_obj_primary_key_col_name, None
        )

        self.assertIsNotNone(schema_obj_foreign_key_column_in_history_table)

        with SessionFactory.using_database(
            SQLAlchemyDatabaseKey.canonical_for_schema(
                schema_type_for_object(expected_schema_object)
            ),
            autocommit=False,
        ) as assert_session:
            history_snapshots: List[DatabaseEntity] = (
                assert_session.query(history_table_class)
                .filter(
                    schema_obj_foreign_key_column_in_history_table
                    == schema_obj_primary_key_value
                )
                .all()
            )

            self.assertEqual(
                len(history_snapshots),
                len(ingest_times),
                f"History snapshots do not correspond to ingest times "
                f"for object of type "
                f"[{expected_schema_object.__class__}]",
            )

            self.assertTrue(
                all(isinstance(s, HistoryTableSharedColumns) for s in history_snapshots)
            )

            def as_history_cols(snapshot: DatabaseEntity) -> HistoryTableSharedColumns:
                if not isinstance(snapshot, HistoryTableSharedColumns):
                    self.fail(
                        f"Snapshot class [{type(snapshot)}] must be a "
                        f"subclass of [{HistoryTableSharedColumns.__name__}]"
                    )
                return snapshot

            history_snapshots.sort(
                key=lambda snapshot: as_history_cols(snapshot).valid_from
            )

            for i, history_snapshot in enumerate(history_snapshots):
                expected_valid_from = ingest_times[i]
                expected_valid_to = (
                    ingest_times[i + 1] if i < len(ingest_times) - 1 else None
                )
                self.assertEqual(
                    expected_valid_from, as_history_cols(history_snapshot).valid_from
                )
                self.assertEqual(
                    expected_valid_to, as_history_cols(history_snapshot).valid_to
                )

            last_history_snapshot = history_snapshots[-1]
            assert last_history_snapshot is not None

            self._assert_schema_object_and_historical_snapshot_match(
                expected_schema_object, last_history_snapshot
            )

    def _assert_schema_object_and_historical_snapshot_match(
        self, schema_object: DatabaseEntity, historical_snapshot: DatabaseEntity
    ) -> None:
        shared_property_names = (
            type(schema_object)
            .get_column_property_names()
            .intersection(type(historical_snapshot).get_column_property_names())
        )
        for column_property_name in shared_property_names:
            expected_col_value = getattr(schema_object, column_property_name)
            historical_col_value = getattr(historical_snapshot, column_property_name)
            self.assertEqual(expected_col_value, historical_col_value)
