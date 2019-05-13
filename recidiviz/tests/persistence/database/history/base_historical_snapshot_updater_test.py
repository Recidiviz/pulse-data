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
"""Base test class for testing subclasses of BaseHistoricalSnapshotUpdater"""
import datetime
from inspect import isclass
from types import ModuleType
from typing import Set, List
from unittest import TestCase

from recidiviz import Session
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.history.historical_snapshot_update import \
    update_historical_snapshots
from recidiviz.persistence.database.schema.schema_person_type import \
    SchemaPersonType
from recidiviz.tests.utils import fakes


class BaseHistoricalSnapshotUpdaterTest(TestCase):
    """
    Base test class for testing subclasses of BaseHistoricalSnapshotUpdater
    """

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    @staticmethod
    def _commit_person(person: SchemaPersonType,
                       system_level: SystemLevel,
                       ingest_time: datetime.datetime):

        act_session = Session()
        act_session.merge(person)

        metadata = IngestMetadata(region='somewhere',
                                  jurisdiction_id='12345',
                                  ingest_time=ingest_time,
                                  system_level=system_level)
        update_historical_snapshots(act_session, [person], [], metadata)

        act_session.commit()
        act_session.close()

    def _check_person_has_relationships_to_all_schema_object_types(
            self,
            person: SchemaPersonType,
            schema: ModuleType,
            entity_type_names_to_ignore: List[str]
    ):

        provided_entity_types = \
            self._get_all_entity_types_in_record_tree(person)

        # Ensure this test covers all entity types
        expected_entity_types = set()
        for attribute_name in dir(schema):
            attribute = getattr(schema, attribute_name)
            # Find all master (non-historical) entity types
            if isclass(attribute) and attribute is not DatabaseEntity and \
                    issubclass(attribute, DatabaseEntity) \
                    and not attribute_name.endswith('History'):
                expected_entity_types.add(attribute_name)

        missing_entity_types = []
        for entity_type in expected_entity_types:
            if entity_type not in provided_entity_types and \
                    entity_type not in entity_type_names_to_ignore:
                missing_entity_types.append(entity_type)
        if missing_entity_types:
            self.fail('Expected entity type(s) {} not found in provided entity '
                      'types'.format(', '.join(missing_entity_types)))

    @staticmethod
    def _get_all_entity_types_in_record_tree(person) -> Set[str]:
        entity_types = set()

        unprocessed = list([person])
        processed = []
        while unprocessed:
            entity = unprocessed.pop()
            entity_types.add(type(entity).__name__)
            processed.append(entity)

            related_entities = []
            for relationship_name in entity.get_relationship_property_names():
                related = getattr(entity, relationship_name)
                # Relationship can return either a list or a single item
                if isinstance(related, list):
                    related_entities.extend(related)
                elif related is not None:
                    related_entities.append(related)

            unprocessed.extend([related_entity for related_entity
                                in related_entities
                                if related_entity not in processed
                                and related_entity not in unprocessed])

        return entity_types

    def _assert_entity_and_snapshot_match(
            self, entity, historical_snapshot) -> None:
        shared_property_names = \
            type(entity).get_column_property_names().intersection(
                type(historical_snapshot).get_column_property_names())
        for column_property_name in shared_property_names:
            entity_value = getattr(entity, column_property_name)
            historical_value = getattr(
                historical_snapshot, column_property_name)
            self.assertEqual(entity_value, historical_value)
